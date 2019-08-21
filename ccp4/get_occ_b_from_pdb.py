import time, sys
import pandas as pd
import numpy as np
from iotbx.pdb import hierarchy
import argparse

def update_from_pdb(pdb_df):
    """
    Find residue name, B factors given DataFrame with LIG

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
    # loop over rows/ residues
    rows = []
    for index, row in pdb_df.iterrows():

        # read into iotbx.hierarchy
        pdb_in = hierarchy.input(file_name=row.pdb_latest)
        # read into iotbx.selection cache
        sel_cache = pdb_in.hierarchy.atom_selection_cache()

        print(row.pdb_latest)
        sel = sel_cache.selection("resname LIG")

        # Select that residue from main hierarchy
        hier = pdb_in.hierarchy.select(sel)

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

                    # copy the row so that the append doesn't
                    # end up appending a series of pointers
                    # to the same object
                    copy_row = row.copy(deep=True)

                    b = []
                    occ = []
                    # Get B factor information on residue by looking a individual atoms
                    for atom in atom_group.atoms():
                        b.append(atom.b)
                        occ.append(atom.occ)

                        # print(atom_group.resname,
                        #       residue_group.resseq,
                        #       atom_group.altloc,
                        #       atom.b,
                        #       atom.occ)

                    occupancy = np.mean(occ)
                    mean_b = np.mean(b)
                    std_b = np.std(b)

                    copy_row['chain'] = chain.id
                    copy_row['resseq'] = residue_group.resseq
                    copy_row['altloc'] = atom_group.altloc
                    copy_row['occupancy'] = occupancy
                    copy_row["B_mean"] = mean_b
                    copy_row["B_std"] = std_b
                    rows.append(copy_row)
        # else:
        #     raise ValueError(
        #         "Multiple residues for selection"
        #         # "chain {} resid {} altloc {} "
        #         # "of pdb: {}".format(row.chain, row.resid, row.alte, pdb)
        #     )

    # Append rows
    pdb_df = pd.concat(rows, axis=1).T

    # As series are single datatype,
    # one should not work row by row
    # This will cause the whole dataframe
    # to be of object datatype.
    # This is a poor quality fix for working row by row
    pdb_df['occupancy'] = pdb_df['occupancy'].astype(float)
    pdb_df['B_mean'] = pdb_df['B_mean'].astype(float)
    pdb_df['B_std'] = pdb_df['B_std'].astype(float)

    # Aggregation can combine rows with different methods.
    # here we sum occupancy across altloc
    # and average the other quantities for the resseq
    pdb_df = pdb_df.groupby(['resseq',
                          'crystal_name',
                          'pdb_latest',
                          'mtz_latest',
                          'refine_log',
                          'chain'], as_index=False).agg({'occupancy':'sum',
                                                              'B_mean':'mean',
                                                              'B_std':'mean'})
    return pdb_df

def main():

    """
    Get occupancy and B from pdbs

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
        "log_occ_correct", help="output csv with log info and resnames", type=str
    )
    args = parser.parse_args()

    pdb_df = pd.read_csv(args.log_occ_csv)
    log_occ_correct_df = update_from_pdb(pdb_df)
    log_occ_correct_df.to_csv(args.log_occ_correct)

if __name__ == "__main__":
    main()