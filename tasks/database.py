import luigi
import pandas as pd
import os

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

    Attributes
    -----------
    log_pdb_mtz_csv: luigi.Parameter()
        path to summary csv contianing at least path to pdb, mtz
        and refinement log file
    test: luigi.Parameter(), optional
        integer representing number of rows to extract when
        used as test
    """
    log_pdb_mtz_csv = luigi.Parameter()
    test = luigi.Parameter(default=None, significant=False)

    def requires(self):
        return None


    def output(self):
        return luigi.LocalTarget(self.log_pdb_mtz_csv)


    def run(self):
        process_refined_crystals(self.log_pdb_mtz_csv, self.test)


class RefineToCsv(luigi.Task):
    """
    Task to get refinement postgres table as csv

    Attributes
    ----------
    refine_csv: luigi.Parameter()
        path to csv file showing refinement table, input


    Methods
    --------
    output()
        output of task is the path to the csv
        of refinement table
    run()
        gets table from postgres database

    """
    refine_csv = luigi.Parameter(default=Path().refine)

    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget(self.refine_csv)

    def run(self):
        refine_df = get_table_df('refinement')

        folder = os.path.dirname(self.refine_csv)
        if not os.path.isdir(folder):
            os.makedirs(folder)

        refine_df.to_csv(self.refine_csv)


@requires(RefineToCsv)
class SuperposedToCsv(luigi.Task):

    """
    Task to get refinements with valid pdb files

    Attributes
    ----------
    superposed_csv: luigi.Parameter()
        path to csv file detailing files that have a superposed pdb file,
         output

    refine_csv: luigi.Parameter()
        path to csv file showing refinemnet table, input

    Methods
    --------
    output()
        output of task is the path to the csv
        of refinement table with only valid pdb files
    run()
        gets table from postgres database, and drops rows
        without superposed pdb files
    """
    superposed_csv = luigi.Parameter(default=Path().superposed)
    refine_csv = luigi.Parameter(default=Path().refine)

    def output(self):
        return luigi.LocalTarget(Path().superposed)


    def run(self):
        #Read in refined data
        refine_df = pd.read_csv(self.refine_csv)

        # Remove rows without a value in pdb column
        pdb_df = refine_df[refine_df.pdb_latest.notnull()]

        # Remove rows without specfied file present on filesystem
        pdb_df = drop_pdb_not_in_filesystem(pdb_df)

        # Remove rows where pdb file is "dimple.pdb"
        superposed_df = drop_only_dimple_processing(pdb_df)

        #Write output table
        superposed_df.to_csv(self.superposed_csv)