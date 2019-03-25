import luigi
import os

from tasks.database import ParseXchemdbToCsv
from tasks.database import SuperposedToCsv
from tasks.update_csv import OccStateComment
from path_config import Path

out_dir = "/dls/science/groups/i04-1/elliot-dev/Work/" \
          "exhaustive_parse_xchem_db/test/"

test_paths = Path()
test_paths.log_pdb_mtz = os.path.join(out_dir, 'log_pdb_mtz.csv')
test_paths.superposed = os.path.join(out_dir, 'superposed.csv')
test_paths.refine = os.path.join(out_dir, 'refine.csv')
test_paths.log_occ_csv = os.path.join(out_dir, 'log_occ.csv')
test_paths.log_occ_resname = os.path.join(out_dir, 'log_occ_resname.csv')
test_paths.occ_state_comment_csv= os.path.join(out_dir, 'occ_state_comment.csv')

luigi.build([ParseXchemdbToCsv(test=5,
                               log_pdb_mtz_csv=test_paths.log_pdb_mtz),

             SuperposedToCsv(superposed_csv=test_paths.superposed,
                             refine_csv=test_paths.refine),

             OccStateComment(occ_state_comment_csv=test_paths.occ_state_comment_csv,
                            log_occ_resname=test_paths.log_occ_resname,
                            log_occ_csv=test_paths.log_occ_csv,
                            log_pdb_mtz_csv=test_paths.log_pdb_mtz,
                            test=5)

             ],
            local_scheduler=True, workers=20)