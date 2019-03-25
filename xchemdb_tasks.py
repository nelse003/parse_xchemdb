import luigi
import pandas as pd
from luigi.util import requires

from parse_xchemdb import process_refined_crystals, get_table_df, drop_pdb_not_in_filesystem, \
    drop_only_dimple_processing
from path_config import Path


class ParseXchemdbToCsv(luigi.Task):
    """Task to parse Postgres tables into csv

    Methods
    --------
    requires()
        No requirements for this task
    output()
        output of task is the path to the csv
        file with refinement and crystal details
        from xchemdb
    run()
        runs parse_xchemdb.process_refined_crystals()

    """
    log_pdb_mtz_csv = luigi.Parameter()
    test = luigi.Parameter(default=None)

    def requires(self):
        return None


    def output(self):
        return luigi.LocalTarget(self.log_pdb_mtz_csv)


    def run(self):
        process_refined_crystals(self.log_pdb_mtz_csv, self.test)


class RefineToDF(luigi.Task):
    """
    Task to get refinement postgres table as csv

    Methods
    --------
    output()
        output of task is the path to the csv
        of refinement table
    run()
        gets table from postgres database

    """
    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget(Path().refine)

    def run(self):
        refine_df = get_table_df('refinement')
        refine_df.to_csv(Path().refine)


@requires(RefineToDF)
class SuperposedToDF(luigi.Task):

    """
    Task to get refinements with valid pdb files

    Methods
    --------
    output()
        output of task is the path to the csv
        of refinement table with only valid pdb files
    run()
        gets table from postgres database
    """
    def output(self):
        return luigi.LocalTarget(Path().superposed)


    def run(self):
        refine_df = pd.read_csv(Path().refine)
        pdb_df = refine_df[refine_df.pdb_latest.notnull()]
        pdb_df = drop_pdb_not_in_filesystem(pdb_df)
        superposed_df = drop_only_dimple_processing(pdb_df)
        superposed_df.to_csv(Path().superposed)