import luigi
import os

from tasks.plotting import SummaryRefinementPlot
from tasks.plotting import PlotConvergenceHistogram
from tasks.plotting import PlotOccConvScatter
from tasks.plotting import PlotGroundOccHistogram
from tasks.plotting import PlotBoundOccHistogram

from tasks.batch import BatchRefinement
from tasks.filesystem import RefinementFolderToCsv
from tasks.update_csv import OccFromLog

from path_config import Path

out_dir = "/dls/science/groups/i04-1/elliot-dev/Work/" \
          "exhaustive_parse_xchem_db/"

# Paths for analysing original refinements
paths = Path()
paths.log_pdb_mtz = os.path.join(out_dir, 'log_pdb_mtz.csv')
paths.superposed = os.path.join(out_dir, 'superposed.csv')
paths.refine = os.path.join(out_dir, 'refine.csv')
paths.log_occ_csv = os.path.join(out_dir, 'log_occ.csv')
paths.log_occ_resname = os.path.join(out_dir, 'log_occ_resname.csv')
paths.occ_state_comment_csv = os.path.join(out_dir, 'occ_state_comment.csv')
paths.refinement_summary = os.path.join(out_dir, 'refinement_summary.csv')
paths.occ_correct_csv = os.path.join(out_dir, 'occ_correct.csv')

paths.refinement_summary_plot = os.path.join(out_dir, 'refinement_summary.png')
paths.convergence_histogram = os.path.join(out_dir, 'convergence_hist.png')
paths.occ_conv_scatter = os.path.join(out_dir, 'occ_conv_scatter.png')
paths.ground_occ_histogram = os.path.join(out_dir, 'occ_ground_histogram.png')
paths.bound_occ_histogram = os.path.join(out_dir, 'occ_bound_histogram.png')

paths.tmp_dir = os.path.join(out_dir, "tmp")
paths.script_dir = "/dls/science/groups/i04-1/elliot-dev/parse_xchemdb"

# Analyse existing refinements
luigi.build([
            # This will also call/test:
            #
            # ResnameToOccLog,
            # OccFromLog,
            # ParseXchemdbToCsv
            # RefineToCsv
            # SuperposedToCsv
            # SummaryRefinement
             SummaryRefinementPlot(occ_state_comment_csv=paths.occ_state_comment_csv,
                                   log_occ_resname=paths.log_occ_resname,
                                   log_occ_csv=paths.log_occ_csv,
                                   log_pdb_mtz_csv=paths.log_pdb_mtz,
                                   superposed_csv=paths.superposed,
                                   refine_csv=paths.refine,
                                   refinement_summary=paths.refinement_summary,
                                   refinement_summary_plot=paths.refinement_summary_plot,
                                   script_path=paths.script_dir),

            # These will also call/test:
            #
            # ResnameToOccLog
            # OccFromLog
            # ParseXchemdbToCsv
            # StateOccupancyToCsv
            PlotConvergenceHistogram(occ_state_comment_csv=paths.occ_state_comment_csv,
                                     log_occ_resname=paths.log_occ_resname,
                                     log_occ_csv=paths.log_occ_csv,
                                     log_pdb_mtz_csv=paths.log_pdb_mtz,
                                     occ_correct_csv=paths.occ_correct_csv,
                                     plot_path = paths.convergence_histogram,
                                     script_path=paths.script_dir),

            PlotOccConvScatter(occ_state_comment_csv=paths.occ_state_comment_csv,
                               log_occ_resname=paths.log_occ_resname,
                               log_occ_csv=paths.log_occ_csv,
                               log_pdb_mtz_csv=paths.log_pdb_mtz,
                               occ_correct_csv=paths.occ_correct_csv,
                               plot_path = paths.occ_conv_scatter,
                               script_path=paths.script_dir),

            PlotGroundOccHistogram(occ_state_comment_csv=paths.occ_state_comment_csv,
                                   log_occ_resname=paths.log_occ_resname,
                                   log_occ_csv=paths.log_occ_csv,
                                   log_pdb_mtz_csv=paths.log_pdb_mtz,
                                   occ_correct_csv=paths.occ_correct_csv,
                                   plot_path=paths.ground_occ_histogram,
                                   script_path=paths.script_dir),

            PlotBoundOccHistogram(occ_state_comment_csv=paths.occ_state_comment_csv,
                                  log_occ_resname=paths.log_occ_resname,
                                  log_occ_csv=paths.log_occ_csv,
                                  log_pdb_mtz_csv=paths.log_pdb_mtz,
                                  occ_correct_csv=paths.occ_correct_csv,
                                  plot_path=paths.bound_occ_histogram,
                                  script_path=paths.script_dir)
             ],
            local_scheduler=True, workers=10)


# Generate new Superposed refinements REFMAC5
paths.convergence_refinement_failures = os.path.join(out_dir,
                            'convergence_refinement_failures.csv')

paths.refinement_dir = os.path.join(out_dir, "convergence_refinement")

paths.convergence_refinement = os.path.join(out_dir,
                                'convergence_refinement.csv')

paths.convergence_occ = os.path.join(out_dir,
                            'convergence_refinement_occ.csv')

paths.convergence_occ_resname = os.path.join(out_dir,
                    'convergence_refinement_occ_resname.csv')

paths.convergence_conv_hist = os.path.join(out_dir,
                            'convergence_conv_hist.png')

paths.convergence_occ_conv_scatter = os.path.join(out_dir,
                            'convergence_occ_conv_scatter.png')

paths.convergence_ground_hist = os.path.join(out_dir,
                            'convergence_ground_occ_hist.png')

paths.convergence_bound_hist = os.path.join(out_dir,
                            'convergence_bound_occ_hist.png')

paths.convergence_occ_correct =os.path.join(out_dir,
                            'convergence_occ_correct.csv')

luigi.build([
        # Calls BatchRefinement
        RefinementFolderToCsv(refinement_csv=paths.convergence_refinement,
                              output_csv=paths.convergence_refinement_failures,
                              out_dir=paths.refinement_dir,
                              tmp_dir=paths.tmp_dir,
                              extra_params="NCYC 3",
                              log_pdb_mtz_csv=paths.log_pdb_mtz,
                              refinement_type="superposed"),

        # These will also call/test:
        # ResnameToOccLog
        # OccFromLog
        # StateOccupancyToCsv
        PlotConvergenceHistogram(occ_state_comment_csv=paths.occ_state_comment_csv,
                                 log_occ_resname=paths.convergence_occ_resname,
                                 log_occ_csv=paths.convergence_occ,
                                 log_pdb_mtz_csv=paths.convergence_refinement,
                                 occ_correct_csv=paths.convergence_occ_correct,
                                 plot_path=paths.convergence_conv_hist,
                                 script_path=paths.script_dir),

        PlotOccConvScatter(occ_state_comment_csv=paths.occ_state_comment_csv,
                           log_occ_resname=paths.convergence_occ_resname,
                           log_occ_csv=paths.convergence_occ,
                           log_pdb_mtz_csv=paths.convergence_refinement,
                           occ_correct_csv=paths.convergence_occ_correct,
                           plot_path=paths.convergence_occ_conv_scatter,
                           script_path=paths.script_dir),

        PlotBoundOccHistogram(occ_state_comment_csv=paths.occ_state_comment_csv,
                              log_occ_resname=paths.convergence_occ_resname,
                              log_occ_csv=paths.convergence_occ,
                              log_pdb_mtz_csv=paths.convergence_refinement,
                              occ_correct_csv=paths.convergence_occ_correct,
                              plot_path=paths.convergence_bound_hist,
                              script_path=paths.script_dir),

        PlotGroundOccHistogram(occ_state_comment_csv=paths.occ_state_comment_csv,
                               log_occ_resname=paths.convergence_occ_resname,
                               log_occ_csv=paths.convergence_occ,
                               log_pdb_mtz_csv=paths.convergence_refinement,
                               occ_correct_csv=paths.convergence_occ_correct,
                               plot_path=paths.convergence_ground_hist,
                               script_path=paths.script_dir)

        ],
    local_scheduler=False, workers=20)

# Generate new unconstrained refinements REFMAC5

paths.bound_refinement_dir = os.path.join(out_dir, "bound_refinement")
paths.bound_refinement_batch_csv = os.path.join(out_dir, 'bound_refmac.csv')
paths.bound_refinement = os.path.join(out_dir, 'bound_refinement_log_pdb_mtz.csv')

luigi.build([
    # Calls BatchRefinement
    RefinementFolderToCsv(refinement_csv=paths.bound_refinement,
                          output_csv=paths.bound_refinement_batch_csv,
                          out_dir=paths.bound_refinement_dir,
                          tmp_dir=paths.tmp_dir,
                          log_pdb_mtz_csv=paths.log_pdb_mtz,
                          ncyc=3,
                          refinement_type="bound")
],
    local_scheduler=True, workers=20)

raise Exception

# Generate new Superposed refinements phenix

luigi.build([
        # Calls BatchRefinement
        RefinementFolderToCsv(refinement_csv=os.path.join(out_dir,
                                "phenix_superposed_log_pdb_mtz.csv"),
                              output_csv=os.path.join(out_dir,
                               "phenix_superposed_batch.csv"),
                              out_dir=os.path.join(out_dir,
                                    "phenix_superposed"),
                              extra_params=None,
                              tmp_dir=paths.tmp_dir,
                              log_pdb_mtz_csv=os.path.join(out_dir,
                                                "log_pdb_mtz.csv"),
                              refinement_program = "phenix",
                              refinement_type="superposed")
                              ],
    local_scheduler=True, workers=10)