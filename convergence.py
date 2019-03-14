import pandas as pd
import numpy as np
import re

pd.options.display.max_columns = 35
pd.options.display.max_colwidth = 50


#############################################################
# Functions designed to be used in pandas.DataFrame.apply
#############################################################

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
        if row['complete group'] == complete_group:
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
    if row['complete group'] == complete_group:
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
    if row['complete group'] == complete_group:
        return comment
    else:
        # Catches AttributeError in teh case where the comment is blank
        try:
            return row.comment
        except AttributeError:
            return None

##############################################################

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

    print(log_path)
    print(type(log_path))

    # Open file and loop over lines
    with open(log_path, 'r') as log:
        for line in log:

            # Log file is group per cycle, with the CGMAT cycle number
            # identifying the cycle begining
            if 'CGMAT cycle number =' in line:
                cycle_line = line
                cycle_int = int(cycle_line.split('=')[-1].lstrip(' ').rstrip('\n'))

            # Get group occupancies by text in the line
            elif 'Group occupances' in line:
                group_occupancies[cycle_int] = [float(group_occ) for group_occ in line.split()[2:]]

            # Get the other information associated with occupancy group,
            # skip lines with 'Data Line' or 'Number' as this is repeated
            # or unnecessary information
            elif "occupancy group" in line:
                    if "Data line" not in line:
                        if "Number" not in line:

                            # Get complete group that the occupancy group belongs to
                            if "complete" in line:
                                complete_groups.append(re.findall(r'\d+', line))
                            else:
                                chain = line.split('chain')[1].split()[0]
                                resid = line.split('resi')[1].split()[0]
                                alte = line.split('alte')[1].split()[0]
                                group = line.split('id')[1].split()[0]
                                occ_group = (chain, resid, alte, group)
                                lig_occ_groups.append(occ_group)

            # Skip through lines in file that have no useful information on
            else:
                continue

    # Merging all group occupancy values into a DataFrame
    df = pd.DataFrame.from_dict(group_occupancies, orient='index')
    # Label columns with cycle number startign at 1
    df.columns = np.arange(1, len(df.columns) + 1)
    # Transpose so cycles are on rows
    df = df.T

    # Create a DataFrame form list of ligand occupancy groups
    df1 = pd.DataFrame(lig_occ_groups, columns=['chain','resid','alte','occupancy group'])
    # Add complete group to each occupancy group in df1
    df1['complete group'] =  df1['occupancy group'].apply(match_occ, complete_groups=complete_groups)

    # reindex df and df1 so that the tables can be joined
    df1.set_index('occupancy group', inplace=True)
    df1.index = df1.index.astype(np.int64)
    df.rename_axis('occupancy group',inplace=True)

    # Join cycle information with residue/ occupancy group information
    occ_group_df = df.join(df1, how='outer')

    return occ_group_df


def process_ground_bound(pdb_df):
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
    pdb_df['state'] = pdb_df.apply(is_lig, axis=1)

    # Loop over the complete groups
    for complete_group in pdb_df['complete group'].unique():

        # Get ligand altlocs
        ligand_altlocs = pdb_df.apply(func=lig_alt,
                                     complete_group=complete_group,
                                     axis=1).dropna().values

        # Update state of rows which are not ligand to "ground" or "bound"
        # dependent on what altloc the ligand is.

        # i.e. LIG is altlocs c,d
        # PHE 226 has altlocs a,b,c,d
        # PHE 226 a,b altlocs are ground
        # PHE 226 c,d altlocs are bound

        pdb_df['state'] = pdb_df.apply(func=altloc_to_state,
                                         complete_group=complete_group,
                                         ligand_altlocs=ligand_altlocs,
                                         axis=1)


        unique_states = pdb_df.loc[
            pdb_df['complete group'] == complete_group]['state'].unique()


        if len(unique_states) == 1:
            # If a complete group contains only bound state add comment
            if unique_states[0] == "bound":
                pdb_df['comment'] = pdb_df.apply(func=comment_cg,
                                                 complete_group=complete_group,
                                                 comment="All bound state",
                                                  axis=1)

            # If a complete group contains only ground state add comment
            if unique_states[0] == "ground":

                # TODO Cases where might also contain LIG not present,
                #  such as NUDT7A_Crude-x2116 where ligand seems to be FRG

                pdb_df['comment'] = pdb_df.apply(func=comment_cg,
                                                 complete_group=complete_group,
                                                 comment="All ground state",
                                                 axis=1)
        elif len(unique_states) == 2:

            # Check that the sum of ground + bound in a complete group is 1.00
            cg_df = pdb_df.loc[pdb_df['complete group'] == complete_group]
            cg_df = cg_df.dropna(axis=1).drop(['chain','complete group',
                                              'resid','resname', 'B_mean',
                                              'state','Unnamed: 0',
                                              'pdb_latest','refine_log',
                                              'occupancy group',
                                              'crystal','B_std'], axis=1)
            # ??
            unique_by_alt_df = cg_df.groupby('alte').nunique().drop(
                ['alte'], axis=1)

            # Insufficent data
            if unique_by_alt_df.empty:
                pdb_df['comment'] = pdb_df.apply(func=comment_cg,
                                                 complete_group=complete_group,
                                                 comment="No occupancy convergence data in log",
                                                 axis=1)

            # Occupancy adds to 1.0, add correctly occupied comemnt
            elif np.unique(unique_by_alt_df.values) == 1:
                pdb_df['comment'] = pdb_df.apply(func=comment_cg,
                                                 complete_group=complete_group,
                                                 comment="Correctly Occupied",
                                                 axis=1)
            else:
                print(pdb_df)
                raise RuntimeError("Error with pdb_df {}".format(pdb_df))

    # All comments need to filled
    if all(pdb_df['comment'].notnull()):
        return pdb_df



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
        occ_df = pd.concat([occ_df],
                           keys=[row.crystal_name],
                           names=['crystal'])
        # Add log info
        occ_df = pd.concat([occ_df],
                           keys=[row.refine_log],
                           names=['refine_log'])
        # Add pdb info
        occ_df = pd.concat([occ_df],
                           keys=[row.pdb_latest],
                           names=['pdb_latest'])
        # Store occ_df
        occ_df_list.append(occ_df)

    # Compile occupancies into single csv
    occ_log_df = pd.concat(occ_df_list)
    occ_log_df.to_csv(log_occ_csv)


def convergence_to_csv(log_labelled_csv, occ_conv_csv):
    """

    Parameters
    ----------
    log_labelled_csv: str
        path to occupancy csv with labelled resnames
    occ_conv_csv:
        path to occupancy_convergence csv


    Returns
    -------

    """

    log_df = pd.read_csv(log_labelled_csv)

    occ_conv_df_list = []

    # Loop over unique pdbs in log_df
    for pos, pdb in enumerate(log_df.pdb_latest.unique()):

        # get subset of dataframe correspond to current pdb
        pdb_df = log_df.loc[log_df['pdb_latest'] == pdb]
        # Add comment and state to each residue in pdb_df
        comment_df = process_ground_bound(pdb_df)
        occ_conv_df_list.append(comment_df)

    # Add all commented df together and output as csv
    occ_conv_summary_df = pd.concat(occ_conv_df_list)
    occ_conv_summary_df.to_csv(occ_conv_csv)
