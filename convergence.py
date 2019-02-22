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

def comment_complete(row, complete_group):
    if row['complete group'] == complete_group:
        return "Complete group is split between ground and bound"
    else:
        try:
            return row.comment
        except AttributeError:
            return None

def comment_all_bound(row, complete_group):
    if row['complete group'] == complete_group:
        return "All bound state"
    else:
        try:
            return row.comment
        except AttributeError:
            return None

def process_ground_bound(pdb_df):

    # TODO Check that the sum of ground + bound in a complete group is close to 1.00

    # TODO Fails at 471

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


        unique_states = pdb_df.loc[pdb_df['complete group'] == complete_group]['state'].unique()
        if len(unique_states) == 1:
            if unique_states[0] == "bound":
                pdb_df['comment'] = pdb_df.apply(func=comment_all_bound,
                                                 complete_group=complete_group,
                                                 axis=1)


        elif len(unique_states) == 2:
                pdb_df['comment'] = pdb_df.apply(func=comment_complete,
                                                 complete_group=complete_group,
                                                 axis=1)

    if all(pdb_df['comment'].notnull()):
        return pdb_df
    else:
        print(pdb_df)
        raise Exception


    #

def get_occupancy_df(log_path, pdb_path, crystal, lig_name='LIG'):

    log_df = read_occ_group_from_refmac_log(log_path=log_path)

    # Get columns associated with bound & ground from
    # ligand groups

    column_bound = list(
        set(occ_conv_df.columns.values).intersection(
            set(lig_groups)))

    print(occ_conv_df)
    print(column_bound)

    if len(column_bound) == 0:
        raise ValueError("No bound state identified")

    column_ground = list(
        set(occ_conv_df.columns.values).symmetric_difference(
            set(lig_groups)))

    print(column_ground)

    raise Exception

    if len(column_bound) == 0:
        raise ValueError("No ground state identified")

    # Add columns together
    ground = np.zeros(len(occ_conv_df))
    bound = np.zeros(len(occ_conv_df))
    for col in column_ground:
        ground += occ_conv_df[col].values
    for col in column_bound:
        bound += occ_conv_df[col].values


    if len(column_ground) > 2:
        multiplicity_ground = len(column_ground) / 2
    else:
        multiplicity_ground = 1
    if len(column_bound) > 2:
        multiplicity_bound = len(column_bound) / 2
    else:
        multiplicity_bound = 1

    occ_conv_df['ground'] = ground / multiplicity_ground
    occ_conv_df['bound'] = bound / multiplicity_bound

    occ_corrected_df = occ_conv_df.drop(column_ground + column_bound, axis=1)

    # Check whether all occupancy values are 1.0
    if len(pd.unique(occ_corrected_df.values)) == 1:
        if pd.unique(occ_corrected_df.values)[0] == 1.0:
            raise ValueError("All occupancies are {}. "
                             "Likely there is no ground state".format(
                pd.unique(occ_corrected_df.values)[0]))


    occ_df = occ_corrected_df.T
    occ_df.reset_index(level=0, inplace=True)
    occ_df.rename(columns={'index': 'state'},
                  index=str,
                  inplace=True)
    occ_df['crystal'] = [crystal, crystal]
    occ_df = occ_df.set_index(['crystal', 'state'])

    # labelling the occupancy with comment, and convergence figure
    comment = {}
    converge = {}

    if len(np.unique(occ_df.values[0])) == 1:
        if np.unique(occ_df.values[0]) == 0:
            comment[(crystal, 'ground')] = "Zero occupancy ground state"
        else:
            comment[(crystal, 'ground')] = \
                "Ground state has single occupancy:{}".format(
                    np.unique(occ_df.values[0]))

    if len(np.unique(occ_df.values[1])) == 1:
        if np.unique(occ_df.values[1]) == 1:
            comment[(crystal, 'bound')] = "Full occupancy bound state"
        else:
            comment[(crystal, 'bound')] = \
                "Bound state has single occupancy: {}".format(
                    np.unique(occ_df.values[1]))

    if not comment:
        for g, b in zip(occ_df.values[0], occ_df.values[1]):
            if g + b > 1.01:
                comment[(crystal, 'ground')] = "Over occupied"
                comment[(crystal, 'bound')] = "Over occupied"
                break
            elif g + b < 0.99:
                comment[(crystal, 'ground')] = "Under occupied"
                comment[(crystal, 'bound')] = "Under occupied"
                break

            comment[(crystal, 'ground')] = "Correctly occupied"
            comment[(crystal, 'bound')] = "Correctly occupied"
            converge[(crystal, 'ground')] = abs(occ_df.values[0][-1] /
                                                occ_df.values[0][-2] - 1)
            converge[(crystal, 'bound')] = abs(occ_df.values[1][-1] /
                                               occ_df.values[1][-2] - 1)

    occ_df['comment'] = occ_df.index
    occ_df['comment'] = occ_df['comment'].map(comment)

    occ_df['converge'] = occ_df.index
    occ_df['converge'] = occ_df['converge'].map(converge)

    occ_df['chain'] = chain
    occ_df['resid'] = resid
    occ_df.reset_index(level=0, inplace=True)
    occ_df.reset_index(level=0, inplace=True)
    occ_df = occ_df.set_index(['crystal', 'state', 'chain', 'resid'])

    return occ_df

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


def main(log_labelled_csv="/dls/science/groups/i04-1/elliot-dev/"
                         "Work/exhaustive_parse_xchem_db/log_test.csv",
         occ_conv_csv="/dls/science/groups/i04-1/elliot-dev/"
                         "Work/exhaustive_parse_xchem_db/occ_conv.csv",
         occ_conv_fails_csv="/dls/science/groups/i04-1/elliot-dev/"
                         "Work/exhaustive_parse_xchem_db/occ_conv_failures.csv"):

    log_df = pd.read_csv(log_labelled_csv)

    occ_conv_df_list = []
    failures = []
    for pos, pdb in enumerate(log_df.pdb_latest.unique()):

        print("******* POS {} ************".format(pos))
        pdb_df = log_df.loc[log_df['pdb_latest'] == pdb]

        comment_df = process_ground_bound(pdb_df)



        # try:
        #
        #
        #     # occ_df = get_occupancy_df(log_path=row.refine_log,
        #     #                           pdb_path=row.pdb_latest,
        #     #                           lig_name='LIG',
        #     #                           crystal=row.crystal_name)
        # except ValueError:
        #     # This is for handling the exception,
        #     # utilising it's traceback to determine
        #     # the consitent errors
        #     #
        #     # This is python 2.7 specific
        #     #
        #     # Could alternatively be done by logging
        #
        #     ex_type, ex, tb = sys.exc_info()
        #     tb_txt = traceback.extract_tb(tb)
        #     error = (row.refine_log, ex_type, ex, tb_txt)
        #     failures.append(error)
        #     print('Error')
        #     continue

        occ_conv_df_list.append(comment_df)

    failures_df = pd.DataFrame(failures, columns=['log',
                                                  'exception_type',
                                                  'exception',
                                                  'traceback'])
    failures_df.to_csv(occ_conv_fails_csv)

    occ_conv_summary_df = pd.concat(occ_conv_df_list)
    occ_conv_summary_df.to_csv(occ_conv_csv)

    len(occ_conv_summary_df)

if __name__ == "__main__":
    """
    Process log files from refmac runs to get occupancy convergence

    Requires ccp4-python

    """
    main()
    # TODO Sort utils/plotting into ccp4 dependent and ccp4 non dependent sections




