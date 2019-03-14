import argparse
import os
from shutil import rmtree
import pandas as pd
import sqlalchemy

# TODO pathlib import

pd.options.display.max_columns = 30
pd.options.display.max_colwidth = 50

def parse_args():
    # TODO Refactor into luigi config
    parser = argparse.ArgumentParser()

    # PanDDA run
    parser.add_argument("-r", "--pandda_run")
    # default="/dls/labxchem/data/2018/lb19758-9/processing/analysis/panddas_run12_all_onlydmso"

    # Output
    parser.add_argument("-o", "--output",
                        default="/dls/science/groups/i04-1/elliot-dev/"
                                "Work/exhaustive_parse_xchem_db")

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

    target = pd.read_sql_query("SELECT * FROM target", con=engine)

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
                 "compounds": compounds,
                 "target":target}

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
        args = parse_args()
    if databases is None:
        databases = get_databases(args)

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

def get_crystal_target(args, databases):

    target_df = get_table_df('target', databases=databases, args=args)
    crystal_df = get_table_df('crystals', databases=databases, args=args)
    crystal_df.set_index('target_id',inplace=True)
    target_df.set_index('id',inplace=True)
    crystal_target_df = crystal_df.join(target_df)
    return crystal_target_df

def process_refined_crystals():

    """Parse XChem postgres table into csv

    All tables in postgres databse are read in.

    Refinement table is the main source of information used.

    Crystal table is merged into refinement table.

    Crystals which do not supply a pdb_latest field are dropped,
    as are those where the file specified in the pdb latest field
    is not a file on the filesystem.

    Parameters
    -----------

    Returns
    ---------
    None
    """

    # get required arguments for loading tables
    args = parse_args()
    databases = get_databases(args)
    refine_df = get_table_df(table_name='refinement',
                              databases=databases,
                              args=args)

    # merge crystal and target information into refine_df
    crystal_target_df = get_crystal_target(args=args, databases=databases)
    crystal_target_df.set_index('id', inplace=True)
    crystal_target_df.rename_axis('crystal_name_id', inplace=True)
    crystal_target_df.drop(['status'],axis=1, inplace=True)
    refine_df.set_index('crystal_name_id', inplace=True)
    refine_df = refine_df.join(crystal_target_df)

    # Drop crystals
    pdb_df = refine_df[refine_df.pdb_latest.notnull()]
    pdb_df = drop_pdb_not_in_filesystem(pdb_df)
    pdb_no_dimple_df = drop_only_dimple_processing(pdb_df)
    pdb_no_dimple_mtz_df = drop_missing_mtz(pdb_no_dimple_df)
    pdb_log_mtz_df = drop_pdb_with_missing_logs(pdb_no_dimple_mtz_df)

    pdb_log_mtz_df.to_csv(os.path.join(args.output, 'log_pdb_mtz.csv'))

def smiles_from_crystal(crystal):

    args = parse_args()
    databases = get_databases(args)

    compounds = get_table_df(table_name='compounds',
                 databases=databases,
                 args=args)

    crystals = get_table_df(table_name='crystals',
                 databases=databases,
                 args=args)

    cmpd_id = crystals.query('crystal_name==@crystal')['compound_id'].values[0]
    smiles = compounds.query('id==@cmpd_id')['smiles'].values[0]

    return smiles

if __name__ == "__main__":


    print(smiles)


