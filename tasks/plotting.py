import luigi
from luigi.util import requires

from tasks import StateOccupancyToCsv

from plotting import refinement_summary_plot
from plotting import occupancy_histogram
from plotting import occupancy_vs_convergence
from plotting import convergence_ratio_histogram

@requires(StateOccupancyToCsv)
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

    def output(self):
        return luigi.LocalTarget(self.plot_path)


class PlotConvergenceHistogram(PlotOccCorrect):
    """
    Plots a histogram of convergence ratio:

    Notes
    ------
    Subclass of PlotOccCorrect, requires

    log_occ_resname: luigi.Parameter()
    occ_conv_csv: luigi.Parameter()
    occ_correct_csv: luigi.Parameter()
    log_pdb_mtz: luigi.Parameter()
    plot_path: luigi.Parameter()
        path to plot file to be plotted

    """
    def run(self):
        convergence_ratio_histogram(occ_correct_csv=self.occ_correct_csv,
                                    plot_path=self.plot_path)


class PlotOccConvScatter(PlotOccCorrect):
    """
    Plots a scatter plot of convergence ratios occupancy:

    Notes
    ------
    Subclass of PlotOccCorrect, requires

    log_occ_resname: luigi.Parameter()
    occ_conv_csv: luigi.Parameter()
    occ_correct_csv: luigi.Parameter()
    log_pdb_mtz: luigi.Parameter()
    plot_path: luigi.Parameter()
        path to plot file to be plotted

    """
    def run(self):
        occupancy_vs_convergence(occ_correct_csv=self.occ_correct_csv,
                                    plot_path=self.plot_path)


class PlotGroundOccHistogram(PlotOccCorrect):
    """
    Plots a histogram of ground state occupancy:

    Notes
    ------
    Subclass of PlotOccCorrect, requires

    log_occ_resname: luigi.Parameter()
    occ_conv_csv: luigi.Parameter()
    occ_correct_csv: luigi.Parameter()
    log_pdb_mtz: luigi.Parameter()
    plot_path: luigi.Parameter()
        path to plot file to be plotted

    """
    def run(self):
        occupancy_histogram(occ_correct_csv=self.occ_correct_csv,
                            plot_path=self.plot_path,
                            state="ground")


class PlotBoundOccHistogram(PlotOccCorrect):
    """
    Plots a histogram of bound state occupancy:

    Notes
    ------
    Subclass of PlotOccCorrect, requires

    log_occ_resname: luigi.Parameter()
    occ_conv_csv: luigi.Parameter()
    occ_correct_csv: luigi.Parameter()
    log_pdb_mtz: luigi.Parameter()
    plot_path: luigi.Parameter()
        path to plot file to be plotted

    """
    def run(self):
        occupancy_histogram(occ_correct_csv=self.occ_correct_csv,
                                  plot_path=self.plot_path,
                                  state="bound")


class SummaryRefinementPlot(luigi.Task):
    """
    Task to produce a plot summarising refinement

    Methods
    --------
    requires()
        csv summarising refinement
    output()
        plot file path
    run()
        plotting from csv
    """

    def requires(self):
        return SummaryRefinement()

    def output(self):
        return luigi.LocalTarget(Path().refinement_summary_plot)

    def run(self):
        refinement_summary_plot(refinement_csv=Path().refinement_summary,
                                out_file_path=Path().refinement_summary_plot)

