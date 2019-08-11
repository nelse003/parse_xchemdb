import pandas as pd
import numpy as np
import re


def match_occ(occ_group, complete_groups):
    """
    Match the occ_group to complete group

    Parameters
    ----------
    occ_group:
        occupancy group label
    complete_groups: list
        list of complete groups

    Returns
    -------
    complete_group:
        complete group that the occupancy_group belongs to
    None
        if occupancy group not in any complete group

    Examples
    ----------
    >>> match_occ(occ_group=1, complete_groups=[[1,2,3,4],[5,6]])
    [1,2,3,4]

    Notes
    ------
    To be used in df.apply()
    """
    for complete_group in complete_groups:
        if occ_group in complete_group:
            return complete_group


def get_occ_from_log(log_pdb_mtz_csv, log_occ_csv):
    """
    For all pdbs in supplied csv read refmac log to get occupancies

    Parameters
    ----------
    log_pdb_mtz_csv: str
        path to input csv of pdb and mtz with quick_refine logs
    log_occ_csv: str
        path to output csv with occupancy information

    Returns
    -------

    """

    log_df = pd.read_csv(log_pdb_mtz_csv)

    occ_df_list = []
    for index, row in log_df.iterrows():
        # Get occupancy from log
        occ_df = read_occ_group_from_refmac_log(log_path=row.refine_log)
        # Add crystal info
        occ_df = pd.concat([occ_df], keys=[row.crystal_name], names=["crystal"])
        # Add log info
        occ_df = pd.concat([occ_df], keys=[row.refine_log], names=["refine_log"])
        # Add pdb info
        occ_df = pd.concat([occ_df], keys=[row.pdb_latest], names=["pdb_latest"])
        # Store occ_df
        occ_df_list.append(occ_df)

    # Compile occupancies into single csv
    occ_log_df = pd.concat(occ_df_list)
    occ_log_df.to_csv(log_occ_csv)


# TODO Write equivalent function to read group occupancies from phenix log file


def read_occ_group_from_refmac_log(log_path):
    """
    Read group occupancy values for cycles of refinement in a refmac log file

    Parameters
    ----------
    log_path: str
        path to the giant.quick-refine or refmac logfile that

    Returns
    -------
    occ_group_df: pandas.DataFrame
        A DataFrame summarising the group occupancy information from the
        refmac logfile. Gives:
            chain,
            altloc (alte),
            residue (resi),
            occupancy group,
            complete group to which the residue belongs to i.e [1,2,3,4]

    Notes
    -----------

    A complete groups should have occupancy that sums to 1.0.
    i.e. [1,2,3,4] should mean that residues in group 1, 2 ,3 and 4
    will add up to 1.0 . In superposed models this is often in paris i.e.
    occupancy groups:
    1: 0.05
    2: 0.05
    3: 0.40
    4: 0.40
    sum to 1.0

    Incomplete occupancy groups do not need to sum to 1.0. However incomplete
    occupancy groups are not generated by giant.split_conformation models.

    """

    # Variables to list values collected whilst looping over the lines in the
    # refmac log file
    lig_occ_groups = []
    complete_groups = []
    group_occupancies = {}

    # Open file and loop over lines
    with open(log_path, "r") as log:
        for line in log:

            # Log file is group per cycle, with the CGMAT cycle number
            # identifying the cycle begining
            if "CGMAT cycle number =" in line:
                cycle_line = line
                cycle_int = int(cycle_line.split("=")[-1].lstrip(" ").rstrip("\n"))

            # Get group occupancies by text in the line
            elif "Group occupances" in line:
                group_occupancies[cycle_int] = [
                    float(group_occ) for group_occ in line.split()[2:]
                ]

            # Get the other information associated with occupancy group,
            # skip lines with 'Data Line' or 'Number' as this is repeated
            # or unnecessary information
            elif "occupancy group" in line:
                if "Data line" not in line:
                    if "Number" not in line:

                        # Get complete group that the occupancy group belongs to
                        if "complete" in line:
                            complete_groups.append(re.findall(r"\d+", line))
                        else:
                            try:
                                chain = line.split("chain")[1].split()[0]
                            except IndexError:
                                chain = None
                            try:
                                resid = line.split("resi")[1].split()[0]
                            except IndexError:
                                resid = None
                            try:
                                alte = line.split("alte")[1].split()[0]
                            except IndexError:
                                alte = None
                            try:
                                group = line.split("id")[1].split()[0]
                            except IndexError:
                                group = None
                            occ_group = (chain, resid, alte, group)
                            lig_occ_groups.append(occ_group)

            # Skip through lines in file that have no useful information on
            else:
                continue

    # Merging all group occupancy values into a DataFrame
    df = pd.DataFrame.from_dict(group_occupancies, orient="index")
    # Label columns with cycle number startign at 1
    df.columns = np.arange(1, len(df.columns) + 1)
    # Transpose so cycles are on rows
    df = df.T

    # Create a DataFrame form list of ligand occupancy groups
    df1 = pd.DataFrame(
        lig_occ_groups, columns=["chain", "resid", "alte", "occupancy group"]
    )
    # Add complete group to each occupancy group in df1
    df1["complete group"] = df1["occupancy group"].apply(
        match_occ, complete_groups=complete_groups
    )

    # reindex df and df1 so that the tables can be joined
    df1.set_index("occupancy group", inplace=True)
    df1.index = df1.index.astype(np.int64)
    df.rename_axis("occupancy group", inplace=True)

    # Join cycle information with residue/ occupancy group information
    occ_group_df = df.join(df1, how="outer")

    return occ_group_df
