import sys
import pandas as pd
import numpy as np
import time
import traceback
import itertools
import re

pd.options.display.max_columns = 35
pd.options.display.max_colwidth = 50

def match_occ(occ_group, complete_groups):
    for group in complete_groups:
        if occ_group in group:
            return group


def read_occ_group_from_refmac_log(log_path):
    lig_occ_groups = []
    complete_groups = []
    group_occupancies = {}
    with open(log_path, 'r') as log:
        for line in log:
            if 'CGMAT cycle number =' in line:
                cycle_line = line
                cycle_int = int(cycle_line.split('=')[-1].lstrip(' ').rstrip('\n'))
            elif 'Group occupances' in line:
                group_occupancies[cycle_int] = [float(group_occ) for group_occ in line.split()[2:]]

            elif "occupancy group" in line:
                    if "Data line" not in line:
                        if "Number" not in line:
                            if "complete" in line:
                                complete_groups.append(re.findall(r'\d+', line))
                            else:
                                chain = line.split('chain')[1].split()[0]
                                resid = line.split('resi')[1].split()[0]
                                alte = line.split('alte')[1].split()[0]
                                group = line.split('id')[1].split()[0]
                                occ_group = (chain, resid, alte, group)
                                lig_occ_groups.append(occ_group)
            else:
                continue

    df = pd.DataFrame.from_dict(group_occupancies, orient='index')
    df.columns = np.arange(1, len(df.columns) + 1)
    df = df.T

    df1 = pd.DataFrame(lig_occ_groups, columns=['chain','resid','alte','occupancy group'])
    df1['complete group'] =  df1['occupancy group'].apply(match_occ, complete_groups=complete_groups)
    df1.set_index('occupancy group', inplace=True)

    df1.index = df1.index.astype(np.int64)
    df.rename_axis('occupancy group',inplace=True)

    occ_group_df = df.join(df1, how='outer')

    return occ_group_df


def drop_similiar_cols(cols, df, atol=0.005):
    for pair_col in itertools.combinations(cols, 2):
        if len(pair_col) == 2:
            col_1 = df[pair_col[0]]
            col_2 = df[pair_col[1]]

            if np.allclose(col_1.values, col_2.values, atol=atol):
                cols_to_drop = pair_col[0]
                return df.drop(cols_to_drop, axis=1)
    return df


def combine_cols(df, cols, col_label):
    col_np_array = np.empty(len(df))
    for col in cols:
        col_np_array += df[col].values
    want_df[col_label] = col_np_array

    return want_df

def is_lig(row):
    if row.resname == "LIG":
        return "bound"

def lig_alt(row, complete_group):
    if row.resname == "LIG":
        if row['complete group'] == complete_group:
            return row.alte

def altloc_to_state(row, complete_group, ligand_altlocs):
    if row['complete group'] == complete_group:
        if row.alte in ligand_altlocs:
            return "bound"
        elif row.alte not in ligand_altlocs:
            return "ground"
    else:
        return row.state

def comment_cg(row, complete_group, comment):
    if row['complete group'] == complete_group:
        return comment
    else:
        try:
            return row.comment
        except AttributeError:
            return None

def process_ground_bound(pdb_df):

    # # Add state to ligand lines
    pdb_df['state'] = pdb_df.apply(is_lig, axis=1)

    for complete_group in pdb_df['complete group'].unique():

        ligand_altlocs = pdb_df.apply(func=lig_alt,
                                     complete_group=complete_group,
                                     axis=1).dropna().values

        pdb_df['state'] = pdb_df.apply(func=altloc_to_state,
                                         complete_group=complete_group,
                                         ligand_altlocs=ligand_altlocs,
                                         axis=1)


        unique_states = pdb_df.loc[
            pdb_df['complete group'] == complete_group]['state'].unique()
        if len(unique_states) == 1:
            if unique_states[0] == "bound":
                pdb_df['comment'] = pdb_df.apply(func=comment_cg,
                                                 complete_group=complete_group,
                                                 comment="All bound state",
                                                 axis=1)
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

            unique_by_alt_df = cg_df.groupby('alte').nunique().drop(
                ['alte'], axis=1)
            if unique_by_alt_df.empty:
                pdb_df['comment'] = pdb_df.apply(func=comment_cg,
                                                 complete_group=complete_group,
                                                 comment="No occupancy convergence data in log",
                                                 axis=1)
            elif np.unique(unique_by_alt_df.values) == 1:
                pdb_df['comment'] = pdb_df.apply(func=comment_cg,
                                                 complete_group=complete_group,
                                                 comment="Correctly Occupied",
                                                 axis=1)
            else:
                print(pdb_df)
                raise Exception

    if all(pdb_df['comment'].notnull()):
        return pdb_df
    else:
        import pdb; pdb.set_trace()
        print(pdb_df)
        raise Exception


def get_occ_from_log(log_pdb_mtz_csv, log_occ_csv):

    log_df = pd.read_csv(log_pdb_mtz_csv)

    occ_df_list = []
    for index, row in log_df.iterrows():
        occ_df = read_occ_group_from_refmac_log(log_path=row.refine_log)
        print(occ_df)
        occ_df = pd.concat([occ_df],
                           keys=[row.crystal_name],
                           names=['crystal'])
        occ_df = pd.concat([occ_df],
                           keys=[row.refine_log],
                           names=['refine_log'])
        occ_df = pd.concat([occ_df],
                           keys=[row.pdb_latest],
                           names=['pdb_latest'])
        occ_df_list.append(occ_df)

    occ_log_df = pd.concat(occ_df_list)
    occ_log_df.to_csv(log_occ_csv)


def main(log_labelled_csv, occ_conv_csv):

    log_df = pd.read_csv(log_labelled_csv)

    occ_conv_df_list = []

    for pos, pdb in enumerate(log_df.pdb_latest.unique()):

        pdb_df = log_df.loc[log_df['pdb_latest'] == pdb]
        comment_df = process_ground_bound(pdb_df)
        occ_conv_df_list.append(comment_df)

    occ_conv_summary_df = pd.concat(occ_conv_df_list)
    occ_conv_summary_df.to_csv(occ_conv_csv)

if __name__ == "__main__":
    """
    Process log files from refmac runs to get occupancy convergence

    Requires ccp4-python

    """
    main()
    # TODO Sort utils/plotting into ccp4 dependent and ccp4 non dependent sections




