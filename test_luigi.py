import luigi
import os

from xchemdb_tasks import ParseXchemdbToCsv
from xchemdb_tasks import SuperposedToDF
from path_config import Path

out_dir = "/dls/science/groups/i04-1/elliot-dev/Work/" \
          "exhaustive_parse_xchem_db/test/"

test_paths = Path()
test_paths.log_pdb_mtz = os.path.join(out_dir, 'log_pdb_mtz.csv')
test_paths.superposed = os.path.join(out_dir, 'superposed.csv')
test_paths.refine = os.path.join(out_dir, 'refine.csv')

luigi.build([ParseXchemdbToCsv(test=5,
                               log_pdb_mtz_csv=test_paths.log_pdb_mtz),

            SuperposedToDF(superposed_csv=test_paths.superposed,
                           refine_csv=test_paths.refine)


            ],
            local_scheduler=True, workers=20)