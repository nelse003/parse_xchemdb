import luigi
import matplotlib
import os
import pandas as pd
import seaborn as sns
from luigi.util import requires

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import itertools
import seaborn as sns

from tasks.update_csv import StateOccupancyToCsv
from tasks.update_csv import SummaryRefinement
from tasks.update_csv import OccFromPdb

from tasks.filesystem import SummariseEdstats

from plotting import refinement_summary_plot
from plotting import occupancy_histogram_from_state_occ
from plotting import occupancy_histogram
from plotting import occupancy_vs_convergence
from plotting import convergence_ratio_histogram
from plotting import convergence_ratio_kde_plot
from plotting import get_state_df


class PlotOccCorrect(luigi.Task):
    # TODO Consder making requires more broad,
    #      allowing any task with sufficient columns
    #      in output table?

    """
    Base Class for plotting functions using occ_correct_csv

    Attributes
    ----------
    plot_path: luigi.Parameter()
        path to plot file to be plotted

    Methods
    --------
    output()
        path to plot

    Notes
    -----
    Cannot pass a function as a luigi parameter, so using inheritance.
    functions below can use this base class

        convergence_ratio_histogram()
        occupancy_vs_convergence()
        bound_state_occ_histogram()
        ground_state_occupancy_histogram()
        
    These are below as:
    
        PlotConvergenceHistogram(PlotOccCorrect)
        PlotOccConvScatter(PlotOccCorrect)
        PlotBoundOccHistogram(PlotOccCorrect)
        PlotGroundOccHistogram(PlotOccCorrect)

    requires StateOccupancyToCsv thus implicit parameters:

        log_occ_resname: luigi.Parameter()
        occ_conv_csv: luigi.Parameter()
        occ_correct_csv: luigi.Parameter()
        log_pdb_mtz: luigi.Parameter()

    """
    plot_path = luigi.Parameter()
    refinement_type = luigi.Parameter(significant=False)
    refinement_program = luigi.Parameter()
    occ_state_comment_csv = luigi.Parameter()
    log_occ_resname = luigi.Parameter()
    log_occ_csv = luigi.Parameter()
    log_pdb_mtz_csv = luigi.Parameter()
    occ_correct_csv = luigi.Parameter()
    script_path = luigi.Parameter()

    def requires(self):
        if "refmac" in self.refinement_program:
            if self.refinement_type == "superposed":
                return StateOccupancyToCsv(
                    occ_state_comment_csv=self.occ_state_comment_csv,
                    log_occ_resname=self.log_occ_resname,
                    log_occ_csv=self.log_occ_csv,
                    log_pdb_mtz_csv=self.log_pdb_mtz_csv,
                    occ_correct_csv=self.occ_correct_csv,
                    refinement_program=self.refinement_program,
                    script_path=self.script_path,
                )
            else:
                return OccFromPdb(
                    log_pdb_mtz_csv=self.log_pdb_mtz_csv,
                    occ_correct_csv=self.occ_correct_csv,
                    script_path=self.script_path,
                )

        elif "phenix" in self.refinement_program:

            return OccFromPdb(
                log_pdb_mtz_csv=self.log_pdb_mtz_csv,
                occ_correct_csv=self.occ_correct_csv,
                script_path=self.script_path,
            )

        elif "buster" in self.refinement_program:

            return OccFromPdb(
                log_pdb_mtz_csv=self.log_pdb_mtz_csv,
                occ_correct_csv=self.occ_correct_csv,
                script_path=self.script_path,
            )

        elif "exhaustive" in self.refinement_program:
            pass

    def output(self):
        return luigi.LocalTarget(self.plot_path)


class PlotConvergenceHistogram(PlotOccCorrect):
    """
    Plots a histogram of convergence ratio:

    Notes
    ------
    Subclass of PlotOccCorrect, requires

    Attributes
    ----------
    Inherited

    occ_state_comment_csv: luigi.Parameter()
         path to csv with state and comment

    log_occ_resname: luigi.Parameter()
         path to csv annotate with residue names for
         residues involved in complete refinment groups,
         as parsed from the REFMAC5 Log file

    log_occ_csv: luigi.Parameter()
        path to csv file where occupancies have
        been obtained from REFMAC logs

    log_pdb_mtz_csv: luigi.Parameter()
        path to summary csv containing at least path to pdb, mtz
        and refinement log file

    test: luigi.Parameter(), optional
        integer representing number of rows to extract when
        used as test

    occ_correct_csv: luigi.Parameter()
        path to csv with convergence information,
        and occupancy for the "bound" or "ground" state

    New

    plot_path: luigi.Parameter()
        path to plot file to be plotted

    """

    def run(self):
        convergence_ratio_histogram(
            occ_state_comment_csv=self.occ_correct_csv, plot_path=self.plot_path
        )


class PlotConvergenceDistPlot(PlotOccCorrect):
    """
    Plots a distplot of two convergence ratios

    Notes
    ------
    Subclass of PlotOccCorrect, requires

    Attributes
    ----------
    occ_state_comment_csv_2: luigi.Parameter()
         path to csv with state and comment to be compared to the first.

    Inherited

    occ_state_comment_csv: luigi.Parameter()
         path to csv with state and comment

    log_occ_resname: luigi.Parameter()
         path to csv annotate with residue names for
         residues involved in complete refinment groups,
         as parsed from the REFMAC5 Log file

    log_occ_csv: luigi.Parameter()
        path to csv file where occupancies have
        been obtained from REFMAC logs

    log_pdb_mtz_csv: luigi.Parameter()
        path to summary csv containing at least path to pdb, mtz
        and refinement log file

    test: luigi.Parameter(), optional
        integer representing number of rows to extract when
        used as test

    occ_correct_csv: luigi.Parameter()
        path to csv with convergence information,
        and occupancy for the "bound" or "ground" state

    New

    plot_path: luigi.Parameter()
        path to plot file to be plotted

    """

    occ_correct_csv_2 = luigi.Parameter()

    def run(self):
        convergence_ratio_kde_plot(
            occ_correct_csv_1=self.occ_correct_csv,
            occ_correct_csv_2=self.occ_correct_csv_2,
            plot_path=self.plot_path,
        )


class PlotEdstatsDistPlot(luigi.Task):
    """
    Plot distplot of all methods for metric
    """
    metric = luigi.Parameter()
    plot_path = luigi.Parameter()
    out_dir = luigi.Parameter()
    xlim = luigi.Parameter(default=None)

    def run(self):

        if not os.path.exists(self.out_dir):
            os.makedirs(self.out_dir)

        if not os.path.exists(os.path.dirname(self.plot_path)):
            os.makedirs(os.path.dirname(self.plot_path))

        method_dict = {
            'refmac_superposed': 'convergence_refinement',
            'refmac': 'bound_refinement',
            'phenix': 'phenix',
            'phenix_superposed': 'phenix_superposed',
            'buster': 'buster',
            'buster_superposed': 'buster_superposed',
            'exhaustive': 'exhaustive',
        }
        fig, ax = plt.subplots()

        metric_dfs = {}
        for method in method_dict.keys():

            # issue with RSZD plots
            if self.metric == "RSZD":
                if "superposed" in method or "exhaustive" in method:
                    continue

            edstats_csv = os.path.join(self.out_dir,
                                       "{method}_edstats.csv".format(
                                           method=method))

            edstats_df = pd.read_csv(edstats_csv)


            if self.xlim is not None:
                plt.xlim(self.xlim)

            ax.spines["right"].set_visible(False)
            ax.spines["top"].set_visible(False)

            metric_dfs[method] = edstats_df[self.metric]

            if self.metric == 'Surroundings B-factor Ratio':
                sns.kdeplot(edstats_df[self.metric].dropna(),
                            gridsize=1000,
                            label=method.replace('_', ' '))
                plt.axvline(x=1,
                            linestyle='--',
                            color='k',
                            linewidth=0.5)
                plt.text(x=0.70,
                         y=2.55,
                         s="Acceptable\ncutoff\nNear 1.0",
                         multialignment='center',
                         fontsize=10)

            elif self.metric == "RSZD":
                pass
            else:
                sns.distplot(edstats_df[self.metric].dropna(),
                             hist=False,
                             label=method.replace('_', ' '))


            plt.ylabel('Density')

            if self.metric == "RSCC":
                plt.axvline(x=0.7,
                            linestyle='--',
                            color='k',
                            linewidth=0.5)
                plt.text(x=0.72,
                         y=5.0,
                         s="Acceptable\ncutoff\n$>$ 0.7",
                         fontsize=10)

            if self.metric == "RSZO/OCC":
                plt.axvline(x=2,
                            linestyle='--',
                            color='k',
                            linewidth=0.5)
                plt.text(x=2.05,
                         y=0.55,
                         s="Acceptable\ncutoff\n$>$ 2.0",
                         fontsize=10,
                         multialignment='left')

        if self.metric == "RSZD":

            plt.hist(
                metric_dfs.values(),
                label = metric_dfs.keys(),
                alpha=0.5,
                bins=100,
                density=True,
                color=[sns.color_palette()[1],
                       sns.color_palette()[2],
                       sns.color_palette()[4]]
                     )

            plt.axvline(x=3,
                        linestyle='--',
                        color='k',
                        linewidth=0.5)

            plt.text(x=2.30,
                     y=0.35,
                     s="Acceptable\ncutoff\n3.0 $<$",
                     fontsize=10,
                     multialignment='right')

            plt.xlabel("RSZD")
            plt.legend()

        plt.savefig(self.plot_path, dpi=300)


class PlotEdstats(luigi.Task):
    """
    Plot multiple instances of PlotEdstatsMetrics
    """
    out_dir = luigi.Parameter()

    def requires(self):
        x_metrics = ['Occupancy',
                     'Average B-factor (Residue)',
                     'Surroundings B-factor Ratio']
        y_metrics = ['RSCC', 'RSZO', 'RSZO/OCC', 'RSZD']

        # method : folder
        method_dict = {
            'refmac_superposed': 'convergence_refinement',
            'refmac': 'bound_refinement',
            'phenix': 'phenix',
            'phenix_superposed': 'phenix_superposed',
            'buster': 'buster',
            'buster_superposed': 'buster_superposed',
            'exhaustive': 'exhaustive',
        }
        tasks = []
        for plot_metric in itertools.product(method_dict.items(),
                                             x_metrics,
                                             y_metrics):
            task = PlotEdstatMetric(
                edstats_csv=os.path.join(self.out_dir,
                                         "{method}_edstats.csv".format(method=plot_metric[0][0])),
                x_metric=plot_metric[1],
                y_metric=plot_metric[2],
                refinement_folder=plot_metric[0][1],
                plot_path=os.path.join(self.out_dir,
                                       "edstats_figures",
                                       "{method}_{x_metric}_{y_metric}.png".format(
                                           method=plot_metric[0][0],
                                           x_metric=plot_metric[1],
                                           y_metric=plot_metric[2].replace('/', '_'))
                                       )
            )
            tasks.append(task)

        return tasks


@requires(SummariseEdstats)
class PlotEdstatMetric(luigi.Task):
    """
    Plot edstats metrics with marginal distributions on scatterplot

    Methods
    -------------
    run()
        plot occ vs RSCC

    Attributes
    -------------
    summary_csv : luigi.Parameter()
        path to exhaustive csv file

    edstats_csv: luigi.Parameter()
        path to edstats summary from edstats

    plot_path: luigi.Parameter()
        path to output plot

    x_metric: luigi.Parameter()
        metric used on x axis

    y_metric: luigi.Parameter()
        metric used on y axis
    """
    edstats_csv = luigi.Parameter()
    plot_path = luigi.Parameter()
    y_metric = luigi.Parameter()
    x_metric = luigi.Parameter()

    def run(self):
        edstats_df = pd.read_csv(self.edstats_csv, index_col=0)

        g = sns.JointGrid(x=edstats_df[self.x_metric],
                          y=edstats_df[self.y_metric])

        g = g.plot_marginals(sns.distplot,
                             color='black',
                             kde=True,
                             hist=False)

        g = g.plot_joint(plt.scatter, alpha=0.5)

        plt.savefig(self.plot_path)


class PlotOccConvScatter(PlotOccCorrect):
    """
    Plots a scatter plot of convergence ratios occupancy:

    Notes
    ------
    Subclass of PlotOccCorrect

    Attributes
    ----------
    Inherited

    occ_state_comment_csv: luigi.Parameter()
         path to csv with state and comment

    log_occ_resname: luigi.Parameter()
         path to csv annotate with residue names for
         residues involved in complete refinment groups,
         as parsed from the REFMAC5 Log file

    log_occ_csv: luigi.Parameter()
        path to csv file where occupancies have
        been obtained from REFMAC logs

    log_pdb_mtz_csv: luigi.Parameter()
        path to summary csv containing at least path to pdb, mtz
        and refinement log file

    test: luigi.Parameter(), optional
        integer representing number of rows to extract when
        used as test

    occ_correct_csv: luigi.Parameter()
        path to csv with convergence information,
        and occupancy for the "bound" or "ground" state

    New

    plot_path: luigi.Parameter()
        path to plot file to be plotted


    """

    def run(self):
        occupancy_vs_convergence(
            occ_correct_csv=self.occ_correct_csv, plot_path=self.plot_path
        )


class PlotOccKde(luigi.Task):
    """
    Plot Kernel density plots of occupancy

    Attributes
    ----------
    refmac_occ_correct_csv : luigi.Parameter()
        path to refmac occ correct csv file

    refmac_superposed_occ_correct_csv : luigi.Parameter()
        path to refmac superposed occ correct csv file

    phenix_superposed_occ_correct_csv : luigi.Parameter()
        path to phenix superposed occ correct csv file

    phenix_occ_correct_csv: luigi.Parameter()
        path to phenix occ correct csv file

    buster_superposed_occ_correct_csv: luigi.Parameter()
        path to buster superposed occ correct csv file

    buster_occ_correct_csv: luigi.Parameter()
        path to buster occ correct csv file

    exhaustive_csv : luigi.Parameter()
        path to exhaustive csv file

    plot_path: luigi.Parameter()
        path to output plot
    """
    refmac_occ_correct_csv = luigi.Parameter()
    refmac_superposed_occ_correct_csv = luigi.Parameter()
    phenix_superposed_occ_correct_csv = luigi.Parameter()
    phenix_occ_correct_csv = luigi.Parameter()
    buster_superposed_occ_correct_csv = luigi.Parameter()
    buster_occ_correct_csv = luigi.Parameter()
    exhaustive_csv = luigi.Parameter()

    plot_path = luigi.Parameter()

    def run(self):
        refmac_df = pd.read_csv(self.refmac_occ_correct_csv)
        refmac_superposed_df = pd.read_csv(self.refmac_superposed_occ_correct_csv)
        phenix_df = pd.read_csv(self.phenix_occ_correct_csv)
        phenix_superposed_df = pd.read_csv(self.phenix_superposed_occ_correct_csv)
        buster_df = pd.read_csv(self.buster_occ_correct_csv)
        buster_superposed_df = pd.read_csv(self.buster_superposed_occ_correct_csv)
        exhaustive_df = pd.read_csv(self.exhaustive_csv)

        refmac_superposed_state_df = get_state_df(occ_correct_df=refmac_superposed_df, state="bound")
        refmac_superposed_occ = refmac_superposed_state_df["state occupancy"]

        plt.xlim(0, 1)

        sns.kdeplot(refmac_superposed_occ, label="refmac superposed")
        sns.kdeplot(refmac_df['occupancy'], label="refmac")
        sns.kdeplot(phenix_df['occupancy'], label="phenix")
        sns.kdeplot(buster_df['occupancy'], label="buster")
        sns.kdeplot(phenix_superposed_df['occupancy'], label="phenix superposed")
        sns.kdeplot(buster_superposed_df['occupancy'], label="buster superposed")
        sns.kdeplot(exhaustive_df['occupancy'], label="exhaustive")

        plt.savefig(self.plot_path)
        plt.close()


class PlotBKde(luigi.Task):
    """
    Plot Kernel density plots of B

    Attributes
    ----------
    refmac_occ_correct_csv : luigi.Parameter()
        path to refmac occ correct csv file

    refmac_superposed_occ_correct_csv : luigi.Parameter()
        path to refmac superposed occ correct csv file

    phenix_superposed_occ_correct_csv : luigi.Parameter()
        path to phenix superposed occ correct csv file

    phenix_occ_correct_csv: luigi.Parameter()
        path to phenix occ correct csv file

    buster_superposed_occ_correct_csv: luigi.Parameter()
        path to buster superposed occ correct csv file

    buster_occ_correct_csv: luigi.Parameter()
        path to buster occ correct csv file

    exhaustive_csv : luigi.Parameter()
        path to exhaustive csv file

    plot_path: luigi.Parameter()
        path to output plot
    """
    refmac_occ_correct_csv = luigi.Parameter()
    refmac_superposed_occ_correct_csv = luigi.Parameter()
    phenix_superposed_occ_correct_csv = luigi.Parameter()
    phenix_occ_correct_csv = luigi.Parameter()
    buster_superposed_occ_correct_csv = luigi.Parameter()
    buster_occ_correct_csv = luigi.Parameter()
    exhaustive_csv = luigi.Parameter()

    plot_path = luigi.Parameter()

    def run(self):
        refmac_df = pd.read_csv(self.refmac_occ_correct_csv)
        refmac_superposed_df = pd.read_csv(self.refmac_superposed_occ_correct_csv)
        phenix_df = pd.read_csv(self.phenix_occ_correct_csv)
        phenix_superposed_df = pd.read_csv(self.phenix_superposed_occ_correct_csv)
        buster_df = pd.read_csv(self.buster_occ_correct_csv)
        buster_superposed_df = pd.read_csv(self.buster_superposed_occ_correct_csv)
        exhaustive_df = pd.read_csv(self.exhaustive_csv)

        refmac_superposed_state_df = get_state_df(occ_correct_df=refmac_superposed_df, state="bound")

        plt.ylabel("Density")
        plt.xlabel("B Factor")

        sns.kdeplot(refmac_superposed_state_df['B_mean'], label="REFMAC5 superposed")
        sns.kdeplot(refmac_df['B_mean'], label="REFMAC5")
        sns.kdeplot(phenix_df['B_mean'], label="phenix.refine")
        sns.kdeplot(buster_df['B_mean'], label="buster refine")
        sns.kdeplot(phenix_superposed_df['B_mean'], label="phenix.refine superposed")
        sns.kdeplot(buster_superposed_df['B_mean'], label="buster superposed")
        sns.kdeplot(exhaustive_df['b_factor'], label="exhaustive")

        plt.savefig(self.plot_path, dpi=300)
        plt.close()


class PlotBViolin(luigi.Task):
    """
    Plot Kernel density plots of B

    Attributes
    ----------
    refmac_occ_correct_csv : luigi.Parameter()
        path to refmac occ correct csv file

    refmac_superposed_occ_correct_csv : luigi.Parameter()
        path to refmac superposed occ correct csv file

    phenix_superposed_occ_correct_csv : luigi.Parameter()
        path to phenix superposed occ correct csv file

    phenix_occ_correct_csv: luigi.Parameter()
        path to phenix occ correct csv file

    buster_superposed_occ_correct_csv: luigi.Parameter()
        path to buster superposed occ correct csv file

    buster_occ_correct_csv: luigi.Parameter()
        path to buster occ correct csv file

    exhaustive_csv : luigi.Parameter()
        path to exhaustive csv file

    plot_path: luigi.Parameter()
        path to output plot
    """
    refmac_occ_correct_csv = luigi.Parameter()
    refmac_superposed_occ_correct_csv = luigi.Parameter()
    phenix_superposed_occ_correct_csv = luigi.Parameter()
    phenix_occ_correct_csv = luigi.Parameter()
    buster_superposed_occ_correct_csv = luigi.Parameter()
    buster_occ_correct_csv = luigi.Parameter()
    exhaustive_csv = luigi.Parameter()

    plot_path = luigi.Parameter()

    def run(self):
        refmac_df = pd.read_csv(self.refmac_occ_correct_csv)
        refmac_superposed_df = pd.read_csv(self.refmac_superposed_occ_correct_csv)
        phenix_df = pd.read_csv(self.phenix_occ_correct_csv)
        phenix_superposed_df = pd.read_csv(self.phenix_superposed_occ_correct_csv)
        buster_df = pd.read_csv(self.buster_occ_correct_csv)
        buster_superposed_df = pd.read_csv(self.buster_superposed_occ_correct_csv)
        exhaustive_df = pd.read_csv(self.exhaustive_csv, index_col=0)

        refmac_superposed_state_df = get_state_df(occ_correct_df=refmac_superposed_df, state="bound")

        fig = plt.subplots(figsize=(10, 10))

        b_dict = {}
        b_dict["REFMAC5 superposed"] = refmac_superposed_state_df['B_mean']
        b_dict["REFMAC5 "] = refmac_df['B_mean']
        b_dict['phenix.refine'] = phenix_df['B_mean']
        b_dict['phenix.refine superposed'] = phenix_superposed_df['B_mean']
        b_dict['buster refine'] = buster_df['B_mean']
        b_dict['buster refine superposed'] = buster_superposed_df['B_mean']
        b_dict['exhaustive'] = exhaustive_df['b_factor']

        b_df = pd.DataFrame.from_dict(b_dict)
        # cut = 0 means no extrapolation from data
        ax = sns.violinplot(data=b_df, cut=0)
        ax.set_xticklabels(ax.get_xticklabels(), rotation=90, fontsize=16)
        plt.yticks(fontsize=16)

        ax.spines["right"].set_visible(False)
        ax.spines["top"].set_visible(False)

        plt.ylabel("B factor", fontsize=18)

        plt.tight_layout()
        plt.savefig(self.plot_path, dpi=300)
        plt.close()


class PlotOccViolin(luigi.Task):
    """
    Plot Kernel density plots of B

    Attributes
    ----------
    refmac_occ_correct_csv : luigi.Parameter()
        path to refmac occ correct csv file

    refmac_superposed_occ_correct_csv : luigi.Parameter()
        path to refmac superposed occ correct csv file

    phenix_superposed_occ_correct_csv : luigi.Parameter()
        path to phenix superposed occ correct csv file

    phenix_occ_correct_csv: luigi.Parameter()
        path to phenix occ correct csv file

    buster_superposed_occ_correct_csv: luigi.Parameter()
        path to buster superposed occ correct csv file

    buster_occ_correct_csv: luigi.Parameter()
        path to buster occ correct csv file

    exhaustive_csv : luigi.Parameter()
        path to exhaustive csv file

    plot_path: luigi.Parameter()
        path to output plot
    """
    refmac_occ_correct_csv = luigi.Parameter()
    refmac_superposed_occ_correct_csv = luigi.Parameter()
    phenix_superposed_occ_correct_csv = luigi.Parameter()
    phenix_occ_correct_csv = luigi.Parameter()
    buster_superposed_occ_correct_csv = luigi.Parameter()
    buster_occ_correct_csv = luigi.Parameter()
    exhaustive_csv = luigi.Parameter()

    plot_path = luigi.Parameter()

    def run(self):
        refmac_df = pd.read_csv(self.refmac_occ_correct_csv)
        refmac_superposed_df = pd.read_csv(self.refmac_superposed_occ_correct_csv)
        phenix_df = pd.read_csv(self.phenix_occ_correct_csv)
        phenix_superposed_df = pd.read_csv(self.phenix_superposed_occ_correct_csv)
        buster_df = pd.read_csv(self.buster_occ_correct_csv)
        buster_superposed_df = pd.read_csv(self.buster_superposed_occ_correct_csv)
        exhaustive_df = pd.read_csv(self.exhaustive_csv, index_col=0)

        refmac_superposed_state_df = get_state_df(occ_correct_df=refmac_superposed_df, state="bound")

        fig = plt.subplots(figsize=(10, 10))

        occ_dict = {}
        occ_dict["REFMAC5 superposed"] = refmac_superposed_state_df['state occupancy']
        occ_dict["REFMAC5"] = refmac_df[refmac_df.occupancy <= 1.0]['occupancy']
        occ_dict['phenix.refine'] = phenix_df['occupancy']
        occ_dict['phenix.refine superposed'] = phenix_superposed_df['occupancy']
        occ_dict['buster refine'] = buster_df['occupancy']
        occ_dict['buster refine superposed'] = buster_superposed_df[buster_superposed_df.occupancy <= 1.0]['occupancy']
        occ_dict['exhaustive'] = exhaustive_df['occupancy']

        print("Refmac 5% {}".format(np.percentile(occ_dict["REFMAC5"], 5)))
        print("Refmac Superposed5% {}".format(np.percentile(occ_dict["REFMAC5 superposed"], 5)))
        print("Buster 5% {}".format(np.percentile(occ_dict["buster refine"], 5)))
        print("Buster superposed 5% {}".format(np.percentile(occ_dict["buster refine superposed"], 5)))
        print("phenix.refine 5% {}".format(np.percentile(occ_dict["phenix.refine"], 5)))
        print("phenix.refine superposed 5% {}".format(np.percentile(occ_dict["phenix.refine superposed"], 5)))
        print("exhaustive 5% {}".format(np.percentile(occ_dict["exhaustive"], 5)))

        occ_df = pd.DataFrame.from_dict(occ_dict)

        ax = sns.violinplot(data=occ_df, cut=0)
        ax.set_xticklabels(ax.get_xticklabels(), rotation=90, fontsize=16)
        plt.yticks(fontsize=16)

        ax.spines["right"].set_visible(False)
        ax.spines["top"].set_visible(False)

        plt.ylabel("Occupancy", fontsize=18)

        plt.tight_layout()
        plt.savefig(self.plot_path, dpi=300)
        plt.close()


class PlotGroundOccHistogram(PlotOccCorrect):
    """
    Plots a histogram of ground state occupancy:

    Notes
    ------
    Subclass of PlotOccCorrect

    Attributes
    ----------
    Inherited

    occ_state_comment_csv: luigi.Parameter()
         path to csv with state and comment

    log_occ_resname: luigi.Parameter()
         path to csv annotate with residue names for
         residues involved in complete refinment groups,
         as parsed from the REFMAC5 Log file

    log_occ_csv: luigi.Parameter()
        path to csv file where occupancies have
        been obtained from REFMAC logs

    log_pdb_mtz_csv: luigi.Parameter()
        path to summary csv containing at least path to pdb, mtz
        and refinement log file

    test: luigi.Parameter(), optional
        integer representing number of rows to extract when
        used as test

    occ_correct_csv: luigi.Parameter()
        path to csv with convergence information,
        and occupancy for the "bound" or "ground" state

    New

    plot_path: luigi.Parameter()
        path to plot file to be plotted


    """

    def run(self):
        occupancy_histogram_from_state_occ(
            occ_correct_csv=self.occ_correct_csv,
            plot_path=self.plot_path,
            state="ground",
        )


class PlotBoundOccHistogram(PlotOccCorrect):
    """
    Plots a histogram of bound state occupancy:

    Notes
    ------
    Subclass of PlotOccCorrect

    Attributes
    ----------
    Inherited

    occ_state_comment_csv: luigi.Parameter()
         path to csv with state and comment

    log_occ_resname: luigi.Parameter()
         path to csv annotate with residue names for
         residues involved in complete refinment groups,
         as parsed from the REFMAC5 Log file

    log_occ_csv: luigi.Parameter()
        path to csv file where occupancies have
        been obtained from REFMAC logs

    log_pdb_mtz_csv: luigi.Parameter()
        path to summary csv containing at least path to pdb, mtz
        and refinement log file

    test: luigi.Parameter(), optional
        integer representing number of rows to extract when
        used as test

    occ_correct_csv: luigi.Parameter()
        path to csv with convergence information,
        and occupancy for the "bound" or "ground" state

    New

    plot_path: luigi.Parameter()
        path to plot file to be plotted


    """

    occ_state_comment_csv = luigi.Parameter(significant=False)
    log_occ_resname = luigi.Parameter(significant=False)
    log_occ_csv = luigi.Parameter(significant=False)
    refinement_program = luigi.Parameter(significant=False)

    def run(self):
        if "refmac" in self.refinement_program:
            if "superposed" in self.refinement_type:
                occupancy_histogram_from_state_occ(
                    occ_correct_csv=self.occ_correct_csv,
                    plot_path=self.plot_path,
                    state="bound",
                )
            else:
                occupancy_histogram(
                    occ_correct_csv=self.occ_correct_csv, plot_path=self.plot_path
                )
        else:
            occupancy_histogram(
                occ_correct_csv=self.occ_correct_csv, plot_path=self.plot_path
            )


@requires(SummaryRefinement)
class SummaryRefinementPlot(luigi.Task):
    """
    Task to produce a plot summarising refinement

    Methods
    --------
    output()
        plot file path
    run()
        plotting from csv

    Notes
    -----
    As requires:

        ResnameToOccLog
        OccFromLog
        ParseXchemdbToCsv
        OccStateComment
        SuperposedToCsv
        SummaryRefinement

    attributes are extended to include those from required tasks

    Attributes
    ----------
    INHERITED:

    occ_state_comment_csv: luigi.Parameter()
         path to csv with state and comment

    log_occ_resname: luigi.Parameter()
         path to csv annotate with residue names for
         residues involved in complete refinment groups,
         as parsed from the REFMAC5 Log file

    log_occ_csv: luigi.Parameter()
        path to csv file where occupancies have
        been obtained from REFMAC logs

    log_pdb_mtz_csv: luigi.Parameter()
        path to summary csv containing at least path to pdb, mtz
        and refinement log file

    test: luigi.Parameter(), optional
        integer representing number of rows to extract when
        used as test

    occ_state_comment_csv: luigi.Parameter()
        path to csv that has state ("bound" or "ground")

    superposed_csv: luigi.Parameter()
        path to csv file detailing files that have a superposed pdb file,
         output

    refine_csv: luigi.Parameter()
        path to csv file showing refinemnet table, input

    refinement_summary: luigi.Parameter()
        path to csv file summarising refinement

    NEW:

    refinement_summary_plot: luigi.Paramter()
        path to plot file

    """

    refinement_summary_plot = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.refinement_summary_plot)

    def run(self):
        refinement_summary_plot(
            refinement_csv=self.refinement_summary,
            out_file_path=self.refinement_summary_plot,
        )
