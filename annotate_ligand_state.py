import pandas as pd
import numpy as np

#############################################################
# Functions designed to be used in pandas.DataFrame.apply
#############################################################


def is_lig(row):
    """
    Check if residue name is LIG

    Parameters
    ----------
    row:
        row of pandas.DataFrame

    Returns
    -------
    str
        "bound" if "LIG" is in resname
    None
        if "LIG is not in resname

    Notes
    -------
    To be used in df.apply()
    """
    if row.resname == "LIG":
        return "bound"


def lig_alt(row, complete_group):
    """
    Get altloc associated with LIG, in supplied complete group

    Parameters
    ----------
    row:
        row of pandas.DataFrame
    complete_group: str
        complete group to check

    Returns
    -------
    row.alte: str
        altloc associated with LIG, in supplied complete group

    Notes
    -------
    To be used in df.apply()
    """
    if row.resname == "LIG":
        if row["complete group"] == complete_group:
            return row.alte


def altloc_to_state(row, complete_group, ligand_altlocs):
    """
    Check if altloc is in ligand altlocs

    Parameters
    ----------
    row:
        row of pandas.DataFrame
    complete_group:
        complete group of occupancy groups
    ligand_altlocs:
        altlocs associated with ligand

    Returns
    -------
    str
        "ground" or "bound" dependent whether altloc is in ligand altlocs

    """
    if row["complete group"] == complete_group:
        if row.alte in ligand_altlocs:
            return "bound"
        elif row.alte not in ligand_altlocs:
            return "ground"
    else:
        return row.state


def comment_cg(row, complete_group, comment):
    """
    Check whether comment in row is equivalent to provided comment string

    Parameters
    ----------
    row:
        row of pandas.DataFrame
    complete_group:
        complete group of occupancy groups to check
    comment: str
        comment string to check

    Returns
    -------
    comment: str

    """
    if row["complete group"] == complete_group:
        return comment
    else:
        # Catches AttributeError in teh case where the comment is blank
        try:
            return row.comment
        except AttributeError:
            return None


##############################################################


def get_state_comment(pdb_df):
    """
    Determine which residues are in "ground" and "bound" states

    Parameters
    ----------
    pdb_df: pandas.DataFrame
        DataFrame of occupancy convergence

    Returns
    -------
    pdb_df: pandas.DataFrame
        DataFrame of occupancy convergence with update comment filed,
        and with a state field

    """

    # Add "bound" to new state column when ligand
    # Add "ground to new state column when not ligand
    pdb_df["state"] = pdb_df.apply(is_lig, axis=1)

    # Loop over the complete groups
    for complete_group in pdb_df["complete group"].unique():

        # Get ligand altlocs
        ligand_altlocs = (
            pdb_df.apply(func=lig_alt, complete_group=complete_group, axis=1)
            .dropna()
            .values
        )

        # Update state of rows which are not ligand to "ground" or "bound"
        # dependent on what altloc the ligand is.

        # i.e. LIG is altlocs c,d
        # PHE 226 has altlocs a,b,c,d
        # PHE 226 a,b altlocs are ground
        # PHE 226 c,d altlocs are bound

        pdb_df["state"] = pdb_df.apply(
            func=altloc_to_state,
            complete_group=complete_group,
            ligand_altlocs=ligand_altlocs,
            axis=1,
        )

        unique_states = pdb_df.loc[pdb_df["complete group"] == complete_group][
            "state"
        ].unique()

        if len(unique_states) == 1:
            # If a complete group contains only bound state add comment
            if unique_states[0] == "bound":
                pdb_df["comment"] = pdb_df.apply(
                    func=comment_cg,
                    complete_group=complete_group,
                    comment="All bound state",
                    axis=1,
                )

            # If a complete group contains only ground state add comment
            elif unique_states[0] == "ground":
                pdb_df["comment"] = pdb_df.apply(
                    func=comment_cg,
                    complete_group=complete_group,
                    comment="All ground state",
                    axis=1,
                )
        elif len(unique_states) == 2:

            # Check that the sum of ground + bound in a complete group is 1.00
            cg_df = pdb_df.loc[pdb_df["complete group"] == complete_group]
            cg_df = cg_df.dropna(axis=1).drop(
                [
                    "chain",
                    "complete group",
                    "resid",
                    "resname",
                    "B_mean",
                    "state",
                    "Unnamed: 0",
                    "pdb_latest",
                    "refine_log",
                    "occupancy group",
                    "crystal",
                    "B_std",
                ],
                axis=1,
            )
            # ??
            unique_by_alt_df = cg_df.groupby("alte").nunique().drop(["alte"], axis=1)

            # Insufficent data
            if unique_by_alt_df.empty:
                pdb_df["comment"] = pdb_df.apply(
                    func=comment_cg,
                    complete_group=complete_group,
                    comment="No occupancy convergence data in log",
                    axis=1,
                )

            # Occupancy adds to 1.0, add correctly occupied comemnt
            elif np.unique(unique_by_alt_df.values) == 1:
                pdb_df["comment"] = pdb_df.apply(
                    func=comment_cg,
                    complete_group=complete_group,
                    comment="Correctly Occupied",
                    axis=1,
                )
            else:
                print(pdb_df)
                raise RuntimeError("Error with pdb_df {}".format(pdb_df))

    # All comments need to filled
    if all(pdb_df["comment"].notnull()):
        return pdb_df


def annotate_csv_with_state_comment(log_occ_resname, occ_conv_csv):
    """
    Loop over pdb files in csv, annotate with comment and state

    Determine whether residues are "bound " or "ground"
    and whether the residues are correctly occupied in a comment field

    Parameters
    ----------
    log_occ_resname: str
        path to occupancy csv with labelled resnames

    occ_conv_csv:
        path to occupancy_convergence csv

    Returns
    -------
    None

    Notes
    -----
    Writes csv to occ_conv_csv
    """

    log_df = pd.read_csv(log_occ_resname)

    occ_conv_df_list = []

    # Loop over unique pdbs in log_df
    for pos, pdb in enumerate(log_df.pdb_latest.unique()):

        # get subset of dataframe correspond to current pdb
        pdb_df = log_df.loc[log_df["pdb_latest"] == pdb]

        # Add comment and state to each residue in pdb_df
        comment_df = get_state_comment(pdb_df)
        occ_conv_df_list.append(comment_df)

    # Add all commented df together and output as csv
    occ_conv_summary_df = pd.concat(occ_conv_df_list)
    occ_conv_summary_df.to_csv(occ_conv_csv)


def state_occ(row, bound, ground, pdb):
    if row.pdb_latest == pdb:
        if row.state == "bound":
            return bound
        if row.state == "ground":
            return ground


def state_occupancies(occ_state_comment_csv, occ_correct_csv):
    """
    Sum occupancies in occupancies for full states

    Adds convergence ratio
    x(n)/(x(n-1) -1)
    to csv.

    Adds up occupancy for ground and bound states respectively
    across each complete group

    Parameters
    ----------
    occ_state_comment_csv: str
        path to csv with occupancy convergence information
        for each residue involved in complete groups
    occ_correct_csv: str
        path to csv with convergence information,
        and occupancy for the "bound" or "ground" state

    Returns
    -------
    None
    """

    # Read CSV
    occ_df = pd.read_csv(occ_state_comment_csv, index_col=[0, 1])

    # Select only residues that are correctly occupied
    occ_correct_df = occ_df[occ_df["comment"] == "Correctly Occupied"]

    # print(occ_correct_df.head())
    # print(occ_correct_df.columns.values)

    # TODO Fix to run where rows are different lengths

    int_cols = []
    for col in occ_correct_df.columns.values:
        try:
            int_cols.append(int(col))
        except ValueError:
            continue
    str_cols = list(map(str, int_cols))
    df = occ_correct_df[str_cols]

    # TODO Find a more robust convergence metric

    occ_correct_df["converge"] = abs(df[str_cols[-1]] / df[str_cols[-2]] - 1)

    # Select the final occupancy value
    occ_correct_df["occupancy"] = df[str_cols[-1]]

    pdb_df_list = []
    for pdb in occ_correct_df.pdb_latest.unique():

        bound = 0
        ground = 0

        pdb_df = occ_correct_df.loc[(occ_correct_df["pdb_latest"] == pdb)]

        grouped = pdb_df.groupby(["complete group", "occupancy", "alte", "state"])

        for name, group in grouped:

            group_occ = group.occupancy.unique()[0]

            if "ground" in group.state.unique()[0]:
                ground += group_occ

            if "bound" in group.state.unique()[0]:
                bound += group_occ

        print(ground + bound)
        try:
            np.testing.assert_allclose(ground + bound, 1.0, atol=0.01)
        except AssertionError:
            continue

        pdb_df["state occupancy"] = pdb_df.apply(
            func=state_occ, bound=bound, ground=ground, pdb=pdb, axis=1
        )

        pdb_df_list.append(pdb_df)

    occ_correct_df = pd.concat(pdb_df_list)
    occ_correct_df.to_csv(occ_correct_csv)


def convergence_state_by_refinement_type(occ_csv, occ_conv_state_csv, refinement_type):

    """
    Add convergence and occupancy and state to csv which has at least convergence columns

    Parameters
    ----------
    occ_csv: str
        Path to input csv which contains occupancy, but not necessarily
    occ_conv_csv: str
        Path to output csv
    refinement_type: str
        "bound" or "ground"

    Returns
    -------

    """
    occ_df = pd.read_csv(occ_csv)

    int_cols = []
    for col in occ_df.columns.values:
        try:
            int_cols.append(int(col))
        except ValueError:
            continue
    str_cols = list(map(str, int_cols))
    df = occ_df[str_cols]

    occ_df["state"] = refinement_type
    occ_df["converge"] = abs(df[str_cols[-1]] / df[str_cols[-2]] - 1)
    occ_df["state occupancy"] = df[str_cols[-1]]

    occ_df.to_csv(occ_conv_state_csv)
