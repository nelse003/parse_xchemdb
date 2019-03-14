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

    # TODO Not sure whehter this is needed
    # Make output directories if not used
    if not os.path.exists(args.output):
        try:
            os.mkdir(args.output)
        except:
            rmtree(args.output)
            os.mkdir(args.output)

    return args

def get_databases(args):

    """
    Get databases as dict of DataFrames from XChemDB postgres tables

    Parameters
    ----------
    args:
        arguments from parse_args, including way to acess databases

    Returns
    -------
    databases: dict of pandas.DataFrames
        dictionary of dataframes each represeneting
        a table from postgres xchemdb
    """

    # Setup access to postgres table
    engine = sqlalchemy.create_engine("postgresql://{}:{}@{}:{}/{}".format(
                                    args.user, args.password, args.host,
                                    args.port, args.database))

    # Use to print all table names
    # print(engine.table_names())

    # Pull out DataFrames from postgres table using supplied engine
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

    # Save as dict of databases
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

def get_table_df(table_name, databases=None, args=None):

    """
    Get DataFrame for table name

    Parameters
    ----------
    table_name: str
        name of table to get
    databases: dict of Pandas.DataFrames, optional
        databases to get, if not supplied these will got
    args: , optional
        arguments from parse_args(), if not supplied parse_args() is run

    Returns
    -------
    df: pandas.DataFrame
        DataFrame corresponding to table
    """

    # Get args and databases if not supplied
    if args is None:
        args = parse_args()
    if databases is None:
        databases = get_databases(args)

    # Get DataFrame from table
    df = databases[table_name]

    return df

def drop_only_dimple_processing(df):

    """
    Drop row if pdb_latest is dimple.pdb

    Parameters
    ----------
    df: pandas.DataFrame
        DataFrame with pdb_latest column

    Returns
    -------
    df: pandas.DataFrame
        DataFrame with rows dropped where pdb_latest contains "dimple"

    """

    dimple_is_latest_ids = []
    for index, row in df.iterrows():
        # Check if pdb_latest contains dimple, i.e. dimple.pdb
        if "dimple" in row.pdb_latest:
            # Add to a list, as we can't remove rows
            # whilst iterating over a dataframe
            dimple_is_latest_ids.append(row.id)

    # Drop rows where id was flagged as having dimple.pdb in pdb_latest column
    df = df[~df['id'].isin(dimple_is_latest_ids)]

    return df

def drop_pdb_not_in_filesystem(df):

    """
    Remove rows in DataFrame where pdb_latest is not on filesystem

    Parameters
    ----------
    df: pandas.DataFrame
        DataFrame containing rows with pdb_latest

    Returns
    -------
    df: pandas.DataFrame
        DataFrame with rows dropped where pdb_latest is not on file system
    """

    missing_ids = []

    for index, row in df.iterrows():
        # Check filesystem for file existence
        if not os.path.isfile(row.pdb_latest):
            # Add to a list, as we can't remove rows
            # whilst iterating over a dataframe
            missing_ids.append(row.id)

    # Drops rows where id is listed in missing_ids
    df = df[~df['id'].isin(missing_ids)]

    return df

def drop_missing_mtz(df):

    """
    Drop rows where mtz_latest is not a valid file

    Parameters
    ----------
    df: pandas.DataFrame
        DataFrame containing rows with pdb_latest

    Returns
    -------
    df: : pandas.DataFrame
        DataFrame with rows dropped where mtz_latest is not on file system

    TODO Implement recursive search if pdb exists,
         as done for QSubRefienement in luig_parsing.py
    """

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

    """
    Drop row from df if quick_refine log file does not exist

    Parameters
    ----------
    df: pandas.DataFrame
        DataFrame
    Returns
    -------
    df: pandas.DataFrame

    """

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
    """
    Join crystal and target tables

    Parameters
    ----------
    args:
        arguments for parse_args
    databases: dict of pandas.DataFrames
        dictionary of dataframes each representing
        a table from postgres xchemdb

    Returns
    -------
    crystal_target_df: pandas.DataFrame
        DataFrame combining the crystal and target tables
    """

    # Get crystal and target dataframes from xchemdb postgres database
    target_df = get_table_df('target', databases=databases, args=args)
    crystal_df = get_table_df('crystals', databases=databases, args=args)

    # Reset index so tables can be joined
    crystal_df.set_index('target_id',inplace=True)
    target_df.set_index('id',inplace=True)

    # Join crystal and target tables
    crystal_target_df = crystal_df.join(target_df)

    return crystal_target_df


def process_refined_crystals(out_csv):

    """Parse XChem postgres table into csv

    All tables in postgres database are read in.

    Refinement table is the main source of information used.

    Crystal table is merged into refinement table.

    Crystals which do not supply a pdb_latest field are dropped,
    as are those where the file specified in the pdb latest field
    is not a file on the filesystem.

    Parameters
    -----------
    out_csv: str
        path to output csv

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

   # Get crystal and target tables combined into df
    crystal_target_df = get_crystal_target(args=args, databases=databases)

    # Reindex crystal_target_df so it can be joined into refine_df
    crystal_target_df.set_index('id', inplace=True)
    crystal_target_df.rename_axis('crystal_name_id', inplace=True)
    crystal_target_df.drop(['status'],axis=1, inplace=True)

    # Reindex refine-df so crystal_target_df can be added
    refine_df.set_index('crystal_name_id', inplace=True)

    # Join crystal and target information into refine_df
    refine_df = refine_df.join(crystal_target_df)

    # Drop crystals where pdb_latest filed is null
    pdb_df = refine_df[refine_df.pdb_latest.notnull()]

    # Also drop crystal where file is not on filesystem
    pdb_df = drop_pdb_not_in_filesystem(pdb_df)

    # Drop rows where the latest pdb is dimple
    pdb_no_dimple_df = drop_only_dimple_processing(pdb_df)

    # Drop rows where mtz is missing
    pdb_no_dimple_mtz_df = drop_missing_mtz(pdb_no_dimple_df)

    # Drop rows where log is missing
    pdb_log_mtz_df = drop_pdb_with_missing_logs(pdb_no_dimple_mtz_df)

    # Write to csv
    pdb_log_mtz_df.to_csv(out_csv)


def smiles_from_crystal(crystal):

    """
    Get smiles string from crystal_name in XChemDB postgres tables

    Parameters
    ----------
    crystal: str
        crystal name

    Returns
    -------
    smiles: str
        smiles string

    Notes
    -----
    This parses the XChemDB postgres table to multiple dataframes
    just to read one line. This will be inefficent,
    but is currently only being called a few times currently
    """

    args = parse_args()
    databases = get_databases(args)

    compounds = get_table_df(table_name='compounds',
                 databases=databases,
                 args=args)

    crystals = get_table_df(table_name='crystals',
                 databases=databases,
                 args=args)

    # Use query to get cmpd_id from crystal table
    cmpd_id = crystals.query('crystal_name==@crystal')['compound_id'].values[0]

    # Use query to get smiles from cmpd_id
    smiles = compounds.query('id==@cmpd_id')['smiles'].values[0]

    return smiles



