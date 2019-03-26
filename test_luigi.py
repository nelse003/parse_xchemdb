import luigi
import os

from tasks.plotting import SummaryRefinementPlot
from path_config import Path

out_dir = "/dls/science/groups/i04-1/elliot-dev/Work/" \
          "exhaustive_parse_xchem_db/test/"

test_paths = Path()
test_paths.log_pdb_mtz = os.path.join(out_dir, 'log_pdb_mtz.csv')
test_paths.superposed = os.path.join(out_dir, 'superposed.csv')
test_paths.refine = os.path.join(out_dir, 'refine.csv')
test_paths.log_occ_csv = os.path.join(out_dir, 'log_occ.csv')
test_paths.log_occ_resname = os.path.join(out_dir, 'log_occ_resname.csv')
test_paths.occ_state_comment_csv = os.path.join(out_dir, 'occ_state_comment.csv')
test_paths.refinement_summary = os.path.join(out_dir, 'refinement_summary.csv')
test_paths.refinement_summary_plot = os.path.join(out_dir, 'refinement_summary.png')

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
                               test=5)

             ],
            local_scheduler=True, workers=20)