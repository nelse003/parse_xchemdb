import os
import pandas as pd

superposed_mtz_log_csv = "/dls/science/groups/i04-1/elliot-dev/Work/" \
                         "exhaustive_parse_xchem_db/log_pdb_mtz.csv"

crystal_csv = "/dls/science/groups/i04-1/elliot-dev/Work/" \
              "exhaustive_parse_xchem_db/crystal.csv"

superposed_df = pd.read_csv(superposed_mtz_log_csv)
crystal_df = pd.read_csv(crystal_csv)

missing_residue_scores = []
score_df_list =[]
print(len(superposed_df))
for index, row in superposed_df.iterrows():
    residue_csv = os.path.join(os.path.dirname(row.pdb_latest), "residue_scores.csv")
    if os.path.isfile(residue_csv):
        scores_df = pd.read_csv(residue_csv)
        scores_df['id'] = row.id
        scores_df.append(score_df_list)
    else:
        missing_residue_scores.append(row.id)

scores_df = pd.concat(scores_df_list)
print(len(missing_residue_scores))