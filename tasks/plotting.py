import luigi
from luigi.util import requires

from tasks.update_csv import StateOccupancyToCsv
from tasks.update_csv import SummaryRefinement

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
        occupancy_histogram(
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

    def run(self):
        occupancy_histogram(
            occ_correct_csv=self.occ_correct_csv,
            plot_path=self.plot_path,
            state="bound",
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
