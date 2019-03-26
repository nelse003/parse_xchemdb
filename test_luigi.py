import luigi
import os

from tasks.plotting import SummaryRefinementPlot
from tasks.plotting import PlotConvergenceHistogram
from tasks.plotting import PlotOccConvScatter
from tasks.plotting import PlotGroundOccHistogram
from tasks.plotting import PlotBoundOccHistogram

from path_config import Path

out_dir = "/dls/science/groups/i04-1/elliot-dev/Work/" \
          "exhaustive_parse_xchem_db/test/"

# Paths for analysing original refinements
test_paths = Path()
test_paths.log_pdb_mtz = os.path.join(out_dir, 'log_pdb_mtz.csv')
test_paths.superposed = os.path.join(out_dir, 'superposed.csv')
test_paths.refine = os.path.join(out_dir, 'refine.csv')
test_paths.log_occ_csv = os.path.join(out_dir, 'log_occ.csv')
test_paths.log_occ_resname = os.path.join(out_dir, 'log_occ_resname.csv')
test_paths.occ_state_comment_csv = os.path.join(out_dir, 'occ_state_comment.csv')
test_paths.refinement_summary = os.path.join(out_dir, 'refinement_summary.csv')
test_paths.occ_correct_csv = os.path.join(out_dir, 'occ_correct.csv')

test_paths.refinement_summary_plot = os.path.join(out_dir, 'refinement_summary.png')
test_paths.convergence_histogram = os.path.join(out_dir, 'convergence_hist.png')
test_paths.occ_conv_scatter = os.path.join(out_dir, 'occ_conv_scatter.png')
test_paths.ground_occ_histogram = os.path.join(out_dir, 'occ_ground_histogram.png')
test_paths.bound_occ_histogram = os.path.join(out_dir, 'occ_bound_histogram.png')

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
             SummaryRefinementPlot(occ_state_comment_csv=test_paths.occ_state_comment_csv,
                               log_occ_resname=test_paths.log_occ_resname,
                               log_occ_csv=test_paths.log_occ_csv,
                               log_pdb_mtz_csv=test_paths.log_pdb_mtz,
                               superposed_csv=test_paths.superposed,
                               refine_csv=test_paths.refine,
                               refinement_summary=test_paths.refinement_summary,
                               refinement_summary_plot=test_paths.refinement_summary_plot,
                               test=5),

            # These will also call/test:
            #
            # ResnameToOccLog
            # OccFromLog
            # ParseXchemdbToCsv
            # StateOccupancyToCsv
            PlotConvergenceHistogram(occ_state_comment_csv=test_paths.occ_state_comment_csv,
                               log_occ_resname=test_paths.log_occ_resname,
                               log_occ_csv=test_paths.log_occ_csv,
                               log_pdb_mtz_csv=test_paths.log_pdb_mtz,
                               occ_correct_csv=test_paths.occ_correct_csv,
                               plot_path = test_paths.convergence_histogram,
                               test=5),
            PlotOccConvScatter(occ_state_comment_csv=test_paths.occ_state_comment_csv,
                               log_occ_resname=test_paths.log_occ_resname,
                               log_occ_csv=test_paths.log_occ_csv,
                               log_pdb_mtz_csv=test_paths.log_pdb_mtz,
                               occ_correct_csv=test_paths.occ_correct_csv,
                               plot_path = test_paths.occ_conv_scatter,
                               test=5),
            PlotGroundOccHistogram(occ_state_comment_csv=test_paths.occ_state_comment_csv,
                               log_occ_resname=test_paths.log_occ_resname,
                               log_occ_csv=test_paths.log_occ_csv,
                               log_pdb_mtz_csv=test_paths.log_pdb_mtz,
                               occ_correct_csv=test_paths.occ_correct_csv,
                               plot_path=test_paths.ground_occ_histogram,
                               test=5),
            PlotBoundOccHistogram(occ_state_comment_csv=test_paths.occ_state_comment_csv,
                           log_occ_resname=test_paths.log_occ_resname,
                           log_occ_csv=test_paths.log_occ_csv,
                           log_pdb_mtz_csv=test_paths.log_pdb_mtz,
                           occ_correct_csv=test_paths.occ_correct_csv,
                           plot_path=test_paths.bound_occ_histogram,
                           test=5)
             ],
            local_scheduler=True, workers=20)

# Generate new Superposed refinements REFMAC5

# Generate new Superposed refinements phenix

# Generate new unconstrained refinements REFMAC5

# Generate new unconstrained refinements phenix

# Generate new exhaustive search refinements

# Generate new unconstrained refinements buster

# Generate new superposed refinements buster