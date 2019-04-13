import os
import pandas as pd
import datetime
from pathlib import Path
import glob

out_dir = "/dls/science/groups/i04-1/elliot-dev/Work/" \
          "exhaustive_parse_xchem_db/test/"
log_pdb_mtz_csv = os.path.join(out_dir, 'log_pdb_mtz.csv')

df = pd.read_csv(log_pdb_mtz_csv)

df = df.tail(300)



for index, row in df.iterrows():
    mod_time = datetime.datetime.fromtimestamp(os.stat(row.pdb_latest).st_mtime)
    if mod_time > datetime.datetime(2019, 3, 27, 0,0,0):
        print(row.pdb_latest)

        pdb_dir = os.path.dirname(row.pdb_latest)

        for f in glob.glob(os.path.join(pdb_dir, "*split.bound-state.pdb")):
            merge_bound = f

        for f in glob.glob(os.path.join(pdb_dir, "*split.ground-state.pdb")):
            merge_ground = f


        if os.path.exists(merge_bound) and os.path.exists(merge_ground):
            os.chdir(pdb_dir)
            os.system('giant.merge_conformations {} {}'.format(merge_bound,
                                                               merge_ground))
            os.rename(os.path.join(pdb_dir, "multi-state-model.pdb"), row.pdb_latest)
