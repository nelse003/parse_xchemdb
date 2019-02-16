import argparse
import os
from shutil import rmtree
import pandas as pd
import sqlalchemy

# TODO pathlib import

pd.options.display.max_columns = 30
pd.options.display.max_colwidth = 50

def parse_args():
    parser = argparse.ArgumentParser()

    # PanDDA run
    parser.add_argument("-r", "--pandda_run")
    # default="/dls/labxchem/data/2018/lb19758-9/processing/analysis/panddas_run12_all_onlydmso"

    # Output
    parser.add_argument("-o", "--output", default="/dls/science/groups/i04-1/elliot-dev/Work/exhaustive_parse_xchem_db")

    # Database
    parser.add_argument("-hs", "--host", default="172.23.142.43")
    parser.add_argument("-p", "--port", default="5432")
    parser.add_argument("-d", "--database", default="test_xchem")
    parser.add_argument("-u", "--user", default="conor")
    parser.add_argument("-pw", "--password", default="c0n0r")

    # Interpeter args
    #parser.add_argument("-cpy", "--ccp4_python", default="/home/zoh22914/myccp4python")

    # Operation
    parser.add_argument("-b", "--build", default=0)


    args = parser.parse_args()

    return args

def get_databases(args):

    engine=sqlalchemy.create_engine("postgresql://{}:{}@{}:{}/{}".format(
                                    args.user, args.password, args.host,
                                    args.port, args.database))

    print(engine.table_names())

    pandda_analysis = pd.read_sql_query("SELECT * FROM pandda_analysis",
                                        con=engine)

    pandda_run = pd.read_sql_query("SELECT * FROM pandda_run", con=engine)

    pandda_events = pd.read_sql_query("SELECT * FROM pandda_event", con=engine)

    pandda_events_stats = pd.read_sql_query("SELECT * FROM pandda_event_stats", con=engine)

    statistical_maps = pd.read_sql_query("SELECT * FROM pandda_statistical_map", con=engine)

    crystals = pd.read_sql_query("SELECT * FROM crystal", con=engine)

    ligands = pd.read_sql_query("SELECT * FROM proasis_hits", con=engine)

    ligand_stats = pd.read_sql_query("SELECT * FROM ligand_edstats", con=engine)

    data_processing = pd.read_sql_query("SELECT * FROM data_processing", con=engine)

    refinement = pd.read_sql_query("SELECT * FROM refinement",con=engine)

    compounds = pd.read_sql_query("SELECT * FROM compounds", con=engine)

    databases = {"pandda_analyses": pandda_analysis,
                 "pandda_runs": pandda_run ,
                 "pandda_events": pandda_events,
                 "pandda_event_stats": pandda_events_stats,
                 "statistical_maps": statistical_maps,
                 "crystals": crystals,
                 "ligands": ligands,
                 "ligand_stats": ligand_stats,
                 "data_processing": data_processing,
                 "refinement": refinement,
                 "compounds": compounds}

    return databases

def prepare_output_folder():

    if not os.path.exists(args.output):
        try:
            os.mkdir(args.output)
        except:
            rmtree(args.output)
            os.mkdir(args.output)

def get_table_df(table_name, databases=None, args=None):

    if args is None:
        parse_args()
    if databases is None:
        get_databases(args)

    args = parse_args()

    if not os.path.exists(args.output):
        try:
            os.mkdir(args.output)
        except:
            rmtree(args.output)
            os.mkdir(args.output)

    df = databases[table_name]

    return df

def drop_only_dimple_processing(df):

    dimple_is_latest_ids = []
    for index, row in df.iterrows():
        if "dimple" in row.pdb_latest:
            dimple_is_latest_ids.append(row.id)

    df = df[~df['id'].isin(dimple_is_latest_ids)]

    return  df

def drop_pdb_not_in_filesystem(df):

    missing_ids = []

    for index, row in df.iterrows():
        if not os.path.isfile(row.pdb_latest):
            missing_ids.append(row.id)

    df = df[~df['id'].isin(missing_ids)]

    return df

def drop_missing_mtz(df):

    missing_mtz_ids = []
    for index, row in df.iterrows():
        if row.mtz_latest is not None:
            if not os.path.isfile(row.mtz_latest):
                missing_mtz_ids.append(row.id)
        else:
            missing_mtz_ids.append(row.id)

    df = df[~df['id'].isin(missing_mtz_ids)]

    return df

def drop_pdb_with_missing_logs(df):

    log_not_found_ids = []
    log_names = {}
    for index, row in df.iterrows():

        dir_name = os.path.dirname(row.pdb_latest)
        base_name = os.path.basename(row.pdb_latest)
        log_name = base_name.replace('.pdb', '.quick-refine.log')
        log_path = os.path.join(dir_name, log_name)

        if not os.path.isfile(log_path):
            log_not_found_ids.append(row.id)
            log_names[row.id] = None
        else:
            log_names[row.id] = log_path

    # drop missing logs
    df = df[~df['id'].isin(log_not_found_ids)]
    # Add log names to df
    df['refine_log'] = df['id'].map(log_names)

    return df

def process_refined_crystals():

    args = parse_args()
    databases = get_databases(args)
    refine_df = get_table_df(table_name='refinement',
                              databases=databases,
                              args=args)
    # Drop crystals
    pdb_df = refine_df[refine_df.pdb_latest.notnull()]

    pdb_df = drop_pdb_not_in_filesystem(pdb_df)
    pdb_no_dimple_df = drop_only_dimple_processing(pdb_df)
    pdb_no_dimple_mtz_df = drop_missing_mtz(pdb_no_dimple_df)
    pdb_log_mtz_df = drop_pdb_with_missing_logs(pdb_no_dimple_mtz_df)
    pdb_log_mtz_df.to_csv(os.path.join(args.output, 'log_pdb_mtz.csv'))


def main():
    """ To Be removed"""

    refine_df = get_table_df(table_name='refinement')
    # drop non null
    pdb_df = refine_df[refine_df.pdb_latest.notnull()]

    print("Length of refinement table: {}".format(len(refine_df)))
    print("Length of refinement table with pdb latest"
          "specified: {}".format(len(pdb_df)))

    # Get only rows with bound conformation
    bound_df = refine_df[refine_df.bound_conf.notnull()]

    print("Length of refinement table with bound state "
          "specified: {}".format(len(bound_df)))

    # drops NaN if both bound_conf and pdb_latest are NaN
    with_pdb_df = refine_df.dropna(axis='index', how='all', subset=['bound_conf', 'pdb_latest'])

    print("Length of refinement table with either bound state "
          "or superposed state specified: {}".format(len(with_pdb_df)))

    # Check path exists for bound, ground and refine.pdb
    bound_missing_ids = []
    superposed_missing_ids = []
    ground_missing_ids = []

    for index, row in with_pdb_df.iterrows():

        if row.bound_conf is not None:
            bound_conf_dir = os.path.dirname(row.bound_conf)
            bound_conf_file_name = os.path.basename(row.bound_conf)
            ground_conf_file_name = bound_conf_file_name.replace('bound', 'ground')
            ground_conf = os.path.join(bound_conf_dir, ground_conf_file_name)

            if not os.path.isfile(row.bound_conf):
                bound_missing_ids.append(row.id)

            if not os.path.isfile(ground_conf):
                ground_missing_ids.append(row.id)
        else:
            bound_missing_ids.append(row.id)
            ground_missing_ids.append(row.id)

        if row.pdb_latest is not None:
            if not os.path.isfile(row.pdb_latest):
                superposed_missing_ids.append(row.id)
        else:
            superposed_missing_ids.append(row.id)

    print("Number of rows where bound structure is not present in filesystem: "
          "{}".format(len(bound_missing_ids)))
    missing_bound_df = with_pdb_df[with_pdb_df['id'].isin(bound_missing_ids)]

    print("Number of rows where latest pdb structure is not present in "
          "filesystem: {}".format(len(superposed_missing_ids)))
    missing_superposed_df = with_pdb_df[with_pdb_df['id'].isin(superposed_missing_ids)]

    print("Number of rows where ground structure is not present in "
          "filesystem: {}".format(len(ground_missing_ids)))
    missing_ground_df = with_pdb_df[with_pdb_df['id'].isin(ground_missing_ids)]

    ground_bound_missing_ids = list(set(bound_missing_ids +
                                        ground_missing_ids))

    missing_ground_bound_df = with_pdb_df[
        with_pdb_df['id'].isin(ground_bound_missing_ids)]

    missing_ids = list(set(bound_missing_ids +
                           ground_missing_ids +
                           superposed_missing_ids))
    # ~ inverts the bool
    with_all_pdb_df = with_pdb_df[~with_pdb_df['id'].isin(missing_ids)]

    print("Number of rows where ground, "
          "bound and latest pdbs exist "
          "in db and filesystem: {}".format(len(with_all_pdb_df)))

    missing_mtz_ids = []
    dimple_is_latest_ids = []
    for index, row in pdb_df.iterrows():
        if row.mtz_latest is not None:
            if not os.path.isfile(row.mtz_latest):
                missing_mtz_ids.append(row.id)
        else:
            missing_mtz_ids.append(row.id)

        if "dimple" in row.pdb_latest:
            dimple_is_latest_ids.append(row.id)

    print("Number of rows with dimple in latest pdb: {}".format(len(dimple_is_latest_ids)))
    print("Number of mtzs that missing: {}".format(len(missing_mtz_ids)))

    # Drop files with dimple in path from superposed list
    superposed_df = superposed_df[
        ~superposed_df['id'].isin(dimple_is_latest_ids)]

    # requires superposed and mtz
    superposed_mtz_df = superposed_df[
        ~superposed_df['id'].isin(missing_mtz_ids)]

    print("Number of rows where superposed pdbs and mtz exist "
          "in db and filesystem: {}".format(len(superposed_mtz_df)))

    log_not_found_ids = []
    log_names = {}
    for index, row in superposed_mtz_df.iterrows():
        # print(row.pdb_latest)
        dir_name = os.path.dirname(row.pdb_latest)
        base_name = os.path.basename(row.pdb_latest)
        log_name = base_name.replace('.pdb', '.quick-refine.log')
        log_path = os.path.join(dir_name, log_name)

        if not os.path.isfile(log_path):
            log_not_found_ids.append(row.id)
            log_names[row.id] = None
        else:
            log_names[row.id] = log_path

        superposed_mtz_log_df = superposed_mtz_df[
            ~superposed_mtz_df['id'].isin(log_not_found_ids)]

    # Add log names to df
    superposed_mtz_log_df['refine_log'] = superposed_mtz_log_df['id'].map(log_names)

    print("Number of rows with superposed pdb, mtz and log: {}".format(
        len(superposed_mtz_log_df)))

    superposed_mtz_log_df.to_csv(os.path.join(args.output, 'log_pdb_mtz.csv'), index=False)




if __name__ == "__main__":
    main()

