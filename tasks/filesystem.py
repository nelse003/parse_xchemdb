import os
import pandas as pd
import luigi
from luigi.util import requires
import numpy as np
import itertools

from utils.filesystem import parse_refinement_folder

import tasks.batch

# TODO Sort out batch follow up
#@requires(tasks.batch.BatchEdstats)

class EdstatsResultSummaries(luigi.Task):
    """Get sumamry from edstats"""
    out_dir = luigi.Parameter()

    def run(self):

        x_metrics = ['Occupancy',
                     'Average B-factor (Residue)',
                     'Surroundings B-factor Ratio']
        y_metrics = ['RSCC','RSZO','RSZO/OCC','RSZD']

        # method : folder
        method_dict = {
            'refmac_superposed': 'convergence_refinement',
            'refmac': 'bound_refinement',
            'phenix':'phenix',
            'phenix_superposed':'phenix_superposed',
            'buster':'buster',
            'buster_superposed': 'buster_superposed',
            'exhaustive':'exhaustive',
        }
        corrs = []
        for (method, folder), x_metric, y_metric in \
            itertools.product(method_dict.items(),
                                  x_metrics,
                                  y_metrics):

            edstats_csv = os.path.join(self.out_dir,
                                       "{method}_edstats.csv".format(
                                           method=method))

            edstats_df = pd.read_csv(edstats_csv)
            corr_df = edstats_df.corr(method='pearson')
            corr_df.to_csv(os.path.join(self.out_dir,
                                        "{method}_corr.csv".format(
                                            method=method)))
            corrs.append(corr_df)

        corr_concat = pd.concat(corrs)
        mean_corr_df = corr_concat.groupby(level=0).mean()
        std_corr_df = corr_concat.groupby(level=0).std()

        mean_corr_df.to_csv(os.path.join(self.out_dir,"corr_mean.csv"))
        std_corr_df.to_csv(os.path.join(self.out_dir,"corr_std.csv"))

        #pd.DataFrame.to_csv()

        RSCC_70_per = []
        RSCC_80_per = []
        RSZO_2 = []
        for method in method_dict.keys():

            edstats_csv = os.path.join(self.out_dir,
                                       "{method}_edstats.csv".format(
                                           method=method))

            edstats_df = pd.read_csv(edstats_csv)

            print("RSCC: {}".format(method))
            rscc = edstats_df["RSCC"].dropna()

            print(len(rscc))
            print(len(rscc[rscc >= 0.7]))
            print(len(rscc[rscc >= 0.7])/len(rscc))
            RSCC_70_per.append(len(rscc[rscc >= 0.7])/len(rscc))

            print(len(rscc[rscc >= 0.8]))
            print(len(rscc[rscc >= 0.8])/len(rscc))
            RSCC_80_per.append(len(rscc[rscc >= 0.8])/len(rscc))
            print("-----------")

            RSZO_OCC = edstats_df["RSZO/OCC"].dropna()

            print("RSZD:" + str(len(edstats_df["RSZD"].dropna())))

        print("Summary 0.7")
        print(np.mean(RSCC_70_per))
        print(np.std(RSCC_70_per))

        print("Summary 0.8")
        print(np.mean(RSCC_80_per))
        print(np.std(RSCC_80_per))

class SummariseEdstats(luigi.Task):
    """Bring all edstats into single file

    Attributes
    --------------
    summary_csv: luigi.Parameter()
        summary edstats csv

    Inherited

    output_csv: luigi.Parameter()
        path to output csv

    refinement_folder: luigi.Parameter()
        path to refinement folder
    """
    edstats_csv = luigi.Parameter()
    refinement_folder = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.edstats_csv)

    def run(self):
        edstats_results = []
        for xtal_folder in os.listdir(self.refinement_folder):

            edstats_csv = os.path.join(self.refinement_folder,
                                       xtal_folder,
                                       "residue_scores.csv")

            if os.path.exists(edstats_csv):
                edstats_results.append(pd.read_csv(edstats_csv))

        edstats_summary_df = pd.concat(edstats_results)
        edstats_summary_df.to_csv(self.edstats_csv)


@requires(tasks.batch.BatchRefinement)
class RefinementFolderToCsv(luigi.Task):

    """Convert refinement folders to CSV

    Parse a refinement folder to get a csv with minimally:
        refine_log: path to refinement log
        crystal_name: crystal name
        pdb_latest: path to
        mtz_latest: path to latest mtz

    The folder structure to be parsed is:
        ./<crystal>/refine.pdb
        ./<crystal>/refine.mtz
        ./<crystal>/refmac.log or ./<crystal>/refine_XXXX/*quick.refine.log

    Methods
    ----------


    Attributes
    -----------
    refinement_csv: luigi.Parameter
        path to csv to store refinement information

    refinement type: luigi.Parameter()
        "ground", "bound" or "superposed" to separate out different
        refinement types

    log_pdb_mtz_csv: luigi.Parameter()
        summary csv contianing at least path to pdb, mtz
        and refinement log file from original refinement/ Database.
        This needs to exist before the batch refinement,
        not be written by it

    out_dir: luigi.Parameter()
        output directory

    tmp_dir: luigi.Parameter()
        temporary directory to hold scripts

    extra_params: luigi.Parameter()
        extra parameters to provide to superposed refinement


    """

    refinement_csv = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.refinement_csv)

    def run(self):
        parse_refinement_folder(
            refinement_dir=self.out_dir,
            refinement_csv=self.refinement_csv,
            refinement_type=self.refinement_type,
        )
