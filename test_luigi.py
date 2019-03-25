import luigi
from xchemdb_tasks import ParseXchemdbToCsv
from path_config import Path

test_paths = Path()
test_paths.log_pdb_mtz = "/dls/science/groups/i04-1/elliot-dev/Work/" \
                    "exhaustive_parse_xchem_db/test/log_pdb_mtz.csv"

luigi.build([ParseXchemdbToCsv(test=5,
                               log_pdb_mtz_csv=test_paths.log_pdb_mtz)],
            local_scheduler=True, workers=20)