import time, sys
import pandas as pd
import numpy as np
from iotbx.pdb import hierarchy
import argparse


def update_progress(progress):

    """ Progress bar

    Notes
    ------------

    https://stackoverflow.com/questions/3160699/python-progress-bar"""

    barLength = 10  # Modify this to change the length of the progress bar
    status = ""
    if isinstance(progress, int):
        progress = float(progress)
    if not isinstance(progress, float):
        progress = 0
        status = "error: progress var must be float\r\n"
    if progress < 0:
        progress = 0
        status = "Halt...\r\n"
    if progress >= 1:
        progress = 1
        status = "Done...\r\n"
    block = int(round(barLength * progress))
    text = "\rPercent: [{0}] {1}% {2}".format(
        "#" * block + "-" * (barLength - block), progress * 100, status
    )
    sys.stdout.write(text)
    sys.stdout.flush()


def update_from_pdb(pdb_df):
    """
    Find residue name, B factors given DataFrame with chain, residue id and altloc

    Carries out cctbx.iotbx dependent searching of pdb file.
    Requires a dataframe where the row has at least,
        pdb_latest: The

    Parameters
    ----------
    pdb_df: Pandas.DataFrame

    Returns
    -------
    pandas.DataFrame:

    """
    # Load pdb path from DataFrame
    # need to select first unique value as there will be duplicates
    # of name for every residue
    pdb = pdb_df.pdb_latest.unique()[0]

    # read into iotbx.hierarchy
    pdb_in = hierarchy.input(file_name=pdb)
    # read into iotbx.selection cache
    sel_cache = pdb_in.hierarchy.atom_selection_cache()

    # loop over rows/ residues
    rows = []
    for index, row in pdb_df.iterrows():

        try:
            # Get selection object which corresponds to supplied chain residue id and altloc
            # Type conversion in res.id neeed otherwise nothing is selected
            sel = sel_cache.selection(
                "chain {} and resid {} and altloc {}".format(
                    row.chain, str(int(row.resid)), row.alte
                )
            )
        except AttributeError:
            # Use ligand LIG instead of chain resid and alte
            # This doesn't work at the next step, a large number
            # are being dropped under "Likely dummy atoms"
            sel = sel_cache.selection("resname LIG")

        # Select that residue from main hierarchy
        hier = pdb_in.hierarchy.select(sel)
        resnames = []

        # catch when multiple models are in pdb file
        try:
            model = hier.only_model()
        except AssertionError:
            pass
        try:
            model = hier.models()[0]
        except IndexError:
            continue

        for chain in model.chains():
            for residue_group in chain.residue_groups():
                for atom_group in residue_group.atom_groups():
                    resnames.append(atom_group.resname)

                    # Get B factor information on residue by looking a individual atoms
                    b = []
                    for atom in atom_group.atoms():
                        b.append(atom.b)

                    mean_b = np.mean(b)
                    std_b = np.std(b)

        # Append information to row
        # if len(resnames) == 1:
        row["resname"] = resnames[0]
        row["B_mean"] = mean_b
        row["B_std"] = std_b
        rows.append(row)
        # else:
        #     raise ValueError(
        #         "Multiple residues for selection"
        #         # "chain {} resid {} altloc {} "
        #         # "of pdb: {}".format(row.chain, row.resid, row.alte, pdb)
        #     )

    # Append rows)
    pdb_df = pd.concat(rows, axis=1)

    # Transpose to get in same orientation as input
    return pdb_df.T


def get_occ_b(pdb, chain, resid, altloc=""):

    """
    Get occupancy and b factor of a single residue

    Parameters
    ----------
    pdb: str
        path to pdb file

    chain: str
        chain of interest

    resid: str
        residue of interest

    altloc: str
        altloc of interest

    Returns
    -------
    mean_occ: float
        mean occupancy of residue

    mean_b: float
        mean b factor of residue

    std_b: float
        standard deviation of b factor of refisude

    """

    # read into iotbx.hierarchy
    pdb_in = hierarchy.input(file_name=pdb)
    # read into iotbx.selection cache
    sel_cache = pdb_in.hierarchy.atom_selection_cache()

    # Get selection object which corresponds to supplied chain residue id and altloc
    sel = sel_cache.selection(
        "chain {} resid {} altloc {}".format(chain, resid, altloc)
    )
    # Select that residue from main hierarchy
    hier = pdb_in.hierarchy.select(sel)
    resnames = []
    for chain in hier.only_model().chains():
        for residue_group in chain.residue_groups():
            for atom_group in residue_group.atom_groups():
                resnames.append(atom_group.resname)

                # Get B factor and occ information on residue by looking a individual atoms
                b = []
                occ = []
                for atom in atom_group.atoms():
                    b.append(atom.b)
                    occ.append(atom.occ)

                    mean_occ = np.mean(occ)
                    mean_b = np.mean(b)
                    std_b = np.std(b)

                    return mean_occ, mean_b, std_b


def get_resname_for_log_occ(log_occ_csv, log_occ_resname_csv):

    """
    Loop pdb files in csv, get residue names, B factors and append to new columns

    Parameters
    ----------
    log_occ_csv: str
        path to input csv
    log_occ_resname_csv: str
        path to output csv: resname, B_mean, B_std columns added

    Returns
    -------
    None

    Notes
    ------
    This is trivially parallelisable and takes ~30 minutes for 3000 crystals.
    TODO Either Parallelise at luigi.Task level or local multiprocessing
    """

    # Read in CSV
    log_df = pd.read_csv(log_occ_csv)

    log_df_list = []
    for pos, pdb in enumerate(log_df.pdb_latest.unique()):

        # Log occ csv contains a line for each residue involved in complete groups,
        # Select residue associated with a pdb file
        pdb_df = log_df.loc[log_df["pdb_latest"] == pdb]

        # Catching assertion error related to multiple models.
        # Only appears to effect a few files, so ignoring,as no obvious reasons,
        # as pdb does nto appear to have multiple models (i.e. NMR like)
        try:
            # Get resnames, mean B factor and B factor standard deviation for
            # each residue involved in complete groups
            pdb_df = update_from_pdb(pdb_df)
        # Dummy atoms might cause duplicate atoms which triggers issue with reading occupancy.
        # Need to measure number of issues
        except ValueError:
            print("Likeley issue with dummy atoms")
            continue
        except AssertionError:
            continue
        # Track progress
        update_progress(float(pos) / float(len(log_df.pdb_latest.unique())))
        # Append to local storage
        log_df_list.append(pdb_df)

    log_occ_resname_df = pd.concat(log_df_list)
    log_occ_resname_df.to_csv(log_occ_resname_csv)


def main():

    """
    Add residue names to occupancy convergence csv

    Returns
    -------
    None

    Notes
    -------
    Written to be called as a command line script using argparser,
    so that it can be called using ccp4-python separately
    from rest of code
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("log_occ_csv", help="input csv with log info", type=str)
    parser.add_argument(
        "log_occ_resname_csv", help="output csv with log info and resnames", type=str
    )
    args = parser.parse_args()

    get_resname_for_log_occ(args.log_occ_csv, args.log_occ_resname_csv)


if __name__ == "__main__":
    main()
