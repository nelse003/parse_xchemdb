import luigi
from luigi.util import requires

from plotting import refinement_summary_plot
from plotting import ground_state_occupancy_histogram
from plotting import bound_state_occ_histogram
from plotting import occupancy_vs_convergence
from plotting import convergence_ratio_histogram

class PlotConvergenceHistogram(luigi.Task):
    """Task to plot histogram of convergence ratios

    Methods
    --------
    requires()
        csv with convergence of occupancies
    output()
        plot file path
    run()
        plotting from csv
    """
    plot_path = luigi.Parameter()
    log_labelled_csv = luigi.Parameter()
    occ_conv_csv = luigi.Parameter()
    occ_correct_csv = luigi.Parameter()
    log_pdb_mtz = luigi.Parameter()

    def requires(self):
        StateOccupancyToCsv(log_labelled_csv=Path().convergence_occ_resname,
                            occ_conv_csv=Path().convergence_occ_conv,
                            log_pdb_mtz=Path().convergence_refinement,
                            occ_correct_csv=Path().convergence_occ_correct)

    def output(self):
        return luigi.LocalTarget(self.plot_path)

    def run(self):
        convergence_ratio_histogram(occ_correct_csv=self.occ_correct_csv,
                                         plot_path=self.plot_path)


class PlotOccConvScatter(luigi.Task):
    """Task to plot scatter of occupancy vs convergence

    Methods
    --------
    requires()
        csv with convergence of occupancies
    output()
        plot file path
    run()
        plotting from csv
    """
    plot_path = luigi.Parameter()
    log_labelled_csv = luigi.Parameter()
    occ_conv_csv = luigi.Parameter()
    occ_correct_csv = luigi.Parameter()
    log_pdb_mtz = luigi.Parameter()

    def requires(self):
        StateOccupancyToCsv(log_labelled_csv=Path().convergence_occ_resname,
                            occ_conv_csv=Path().convergence_occ_conv,
                            log_pdb_mtz=Path().convergence_refinement,
                            occ_correct_csv=Path().convergence_occ_correct)

    def output(self):
        return luigi.LocalTarget(self.plot_path)

    def run(self):
        occupancy_vs_convergence(occ_correct_csv=self.occ_correct_csv,
                                         plot_path=self.plot_path)

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


class PlotGroundOccHistogram(luigi.Task):

    """Task to plot histogram of ground occupancies

    Methods
    --------
    requires()
        csv with convergence of occupancies
    output()
        plot file path
    run()
        plotting from csv
    """
    plot_path = luigi.Parameter()
    log_labelled_csv = luigi.Parameter()
    occ_conv_csv = luigi.Parameter()
    occ_correct_csv = luigi.Parameter()
    log_pdb_mtz = luigi.Parameter()

    def requires(self):
        StateOccupancyToCsv(log_labelled_csv=Path().convergence_occ_resname,
                            occ_conv_csv=Path().convergence_occ_conv,
                            log_pdb_mtz=Path().convergence_refinement,
                            occ_correct_csv=Path().convergence_occ_correct)

    def output(self):
        return luigi.LocalTarget(self.plot_path)

    def run(self):
        ground_state_occupancy_histogram(occ_correct_csv=self.occ_correct_csv,
                                         plot_path=self.plot_path)


class PlotBoundOccHistogram(luigi.Task):
    """Task to plot histogram of bound occupancies

    Methods
    --------
    requires()
        csv with convergence of occupancies
    output()
        plot file path
    run()
        plotting from csv
    """
    plot_path = luigi.Parameter()
    log_labelled_csv = luigi.Parameter()
    occ_conv_csv = luigi.Parameter()
    occ_correct_csv = luigi.Parameter()
    log_pdb_mtz = luigi.Parameter()

    def requires(self):
        StateOccupancyToCsv(log_labelled_csv=Path().convergence_occ_resname,
                            occ_conv_csv=Path().convergence_occ_conv,
                            log_pdb_mtz=Path().convergence_refinement,
                            occ_correct_csv=Path().convergence_occ_correct)

    def output(self):
        return luigi.LocalTarget(self.plot_path)

    def run(self):
        bound_state_occ_histogram(occ_correct_csv=self.occ_correct_csv,
                                  plot_path=self.plot_path)