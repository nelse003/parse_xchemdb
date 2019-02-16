import sys

sys.path.append('/dls/science/groups/i04-1/elliot-dev/Work/exhaustive_search')

import pandas as pd
import numpy as np
import time
import iotbx.pdb
import sys
import traceback
import itertools

from exhaustive.exhaustive.plotting.plot import plot_occupancy_convergence


def read_occupancies_from_refmac_log(log_path):
    """Read group occupancies from log

    Line to be read from is Group occupances  0.12964654  0.122 ...

    Parameters
    -----------
    log_path: str
        Path to log file

    Returns
    -----------
    A dataframe of columns = cycles, rows= occ groups

    """
    group_occupancies = {}
    with open(log_path, 'r') as log:
        for line in log:
            if 'CGMAT cycle number =' in line:
                cycle_line = line
                cycle_int = int(cycle_line.split('=')[-1].lstrip(' ').rstrip('\n'))
            if 'Group occupances' in line:
                group_occupancies[cycle_int] = [float(group_occ) for group_occ in line.split()[2:]]

    df = pd.DataFrame.from_dict(group_occupancies, orient='index')
    df.columns = np.arange(1, len(df.columns) + 1)

    return df


def read_lig_occ_group_from_refmac_log(log_path, lig_chain):
    lig_occ_groups = []
    with open(log_path, 'r') as log:
        for line in log:
            if "occupancy group" in line:
                if "chain {}".format(lig_chain) in line:
                    if "Data line" not in line:
                        lig_occ_groups.append(line.split('id')[1].split()[0])

    return lig_occ_groups


# TODO Replace with sourcing from xchem database table
def pdb_path_to_crystal_target(pdb_path):
    pdb_path_split = pdb_path.split('/')
    crystal_name = [path_part for path_part in pdb_path_split if '-x' in path_part]
    print(crystal_name)
    if len(crystal_name) == 0:
        raise ValueError("Failing to parse crystal name "
                         "from pdb: {}".format(pdb_path))
    crystal_name = crystal_name[0]
    target = crystal_name.split('-')[0]

    return crystal_name, target


def get_lig_chain_id(pdb_path, lig_name='LIG'):
    """Get chain which ligand is in

    Notes
    --------------
    Requires ccp4-python (iotbx.pdb)
    """

    pdb_in = iotbx.pdb.hierarchy.input(file_name=pdb_path)
    sel_cache = pdb_in.hierarchy.atom_selection_cache()
    lig_sel = sel_cache.selection("resname {}".format(lig_name))
    lig_hierarchy = pdb_in.hierarchy.select(lig_sel)
    lig_chains = []

    if lig_hierarchy.models_size() == 0:
        raise ValueError("No ligand found under name: {}".format(lig_name))

    for chain in lig_hierarchy.only_model().chains():
        lig_chains.append(chain.id)

    if len(lig_chains) == 1:
        return chain.id
    else:
        raise ValueError('Ligand chain is not unique: {}'.format(lig_chains))


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


def get_occupancy_df(log_path, pdb_path, crystal, lig_name='LIG', ):
    chain = get_lig_chain_id(pdb_path=pdb_path, lig_name=lig_name)

    lig_groups = read_lig_occ_group_from_refmac_log(log_path=log_path,
                                                    lig_chain=chain)
    lig_groups = np.asarray(lig_groups, dtype=np.int64)

    occ_conv_df = read_occupancies_from_refmac_log(log_path)

    # Get columns associated with bound & ground from
    # ligand groups

    column_bound = list(
        set(occ_conv_df.columns.values).intersection(
            set(lig_groups)))

    if len(column_bound) == 0:
        raise ValueError("No bound state identified")

    column_ground = list(
        set(occ_conv_df.columns.values).symmetric_difference(
            set(lig_groups)))

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
    occ_corrected_df = occ_conv_df.drop(column_ground + column_bound,
                                        axis=1)

    # Check whether all occupancy values are 1.0
    if len(pd.unique(occ_corrected_df.values)) == 1:
        if pd.unique(occ_corrected_df.values)[0] == 1.0:
            raise ValueError("All occupancies are {}. "
                             "Likely there is no ground state".format(
                pd.unique(occ_corrected_df.values)[0]))

    if len(occ_corrected_df.T) != 2:
        raise ValueError("Too many columns in occupancy df".format(occ_corrected_df))

    # Transposing and adding crystal_id
    occ_df = occ_corrected_df.T
    occ_df.reset_index(level=0, inplace=True)
    occ_df.rename(columns={'index': 'state'},
                  index=str,
                  inplace=True)
    occ_df['crystal'] = [crystal, crystal]
    occ_df = occ_df.set_index(['crystal', 'state'])

    return occ_df


if __name__ == "__main__":
    """
    Process log files from refmac runs to get occupancy convergence

    Requires ccp4-python

    """

    # TODO Sort utils/plotting into ccp4 dependent and ccp4 non dependent sections

    log_csv = "/dls/science/groups/i04-1/elliot-dev/Work/" \
              "exhaustive_parse_xchem_db/log_pdb_mtz.csv"

    occ_conv_csv = "/dls/science/groups/i04-1/elliot-dev/Work/" \
                   "exhaustive_parse_xchem_db/occ_conv.csv"

    occ_conv_fails_csv = "/dls/science/groups/i04-1/elliot-dev/Work/" \
                         "exhaustive_parse_xchem_db/occ_conv_failures.csv"

    log_df = pd.read_csv(log_csv)

    occ_conv_df_list = []
    failures = []
    for index, row in log_df.iterrows():
        print(row.refine_log)
        try:
            crystal, target = pdb_path_to_crystal_target(row.pdb_latest)
            occ_df = get_occupancy_df(log_path=row.refine_log,
                                      pdb_path=row.pdb_latest,
                                      lig_name='LIG',
                                      crystal=crystal)
        except ValueError:
            # This is for handling the exception,
            # utilising it's traceback to determine
            # the consitent errors
            #
            # This is python 2.7 specific
            #
            # Could alternatively be done by logging

            ex_type, ex, tb = sys.exc_info()
            tb_txt = traceback.extract_tb(tb)
            error = (row.refine_log, ex_type, ex, tb_txt)
            failures.append(error)
            print('Error')
            continue

        # labelling the occupancy with comment, and convergence figure
        comment = {}
        converge = {}
        if all(occ_df.values[0]) == 0:
            comment[(crystal, 'ground')] = "Zero occupancy ground state"

        elif len(np.unique(occ_df.values[0])) == 1:
            comment[(crystal, 'ground')] = "Ground state has single occupancy: " \
                                           "{}".format(np.unique(occ_df.values[0]))

        if all(occ_df.values[1]) == 1:
            comment[(crystal, 'bound')] = "Full occupancy bound state"

        elif len(np.unique(occ_df.values[1])) == 1:
            comment[(crystal, 'bound')] = "Bound state has single occupancy: " \
                                          "{}".format(np.unique(occ_df.values[0]))

        if len(comment) != 2:
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

        occ_conv_df_list.append(occ_df)

    failures_df = pd.DataFrame(failures, columns=['log',
                                                  'exception_type',
                                                  'exception',
                                                  'traceback'])
    failures_df.to_csv(occ_conv_fails_csv)

    occ_conv_summary_df = pd.concat(occ_conv_df_list)
    occ_conv_summary_df.to_csv(occ_conv_csv)

    len(occ_conv_summary_df)

