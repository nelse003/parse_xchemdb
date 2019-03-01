import pandas as pd

def check_str(s, start):
    if s.startswith(start):
        return start
    else:
        return s

def refinement_summary(occ_conv_csv,
                       refine_csv,
                       superposed_csv,
                       log_pdb_mtz_csv,
                       out_csv):

    occ_conv_df = pd.read_csv(occ_conv_csv)
    refine_df = pd.read_csv(refine_csv)
    superposed_df = pd.read_csv(superposed_csv)
    log_pdb_mtz_df = pd.read_csv(log_pdb_mtz_csv)
    pdb_df = refine_df[refine_df.pdb_latest.notnull()]

    comments = occ_conv_df[['comment','crystal','complete group']]
    comments = comments.drop_duplicates()
    summary = comments['comment'].value_counts()

    summary_dict = {}
    summary_dict['Refinement latest is dimple'] = len(pdb_df)-len(superposed_df)
    summary_dict['Refinement log is missing'] = len(superposed_df) - len(log_pdb_mtz_df)
    summary = summary.append(pd.Series(summary_dict))

    print(summary)

    summary.to_csv(out_csv, header=False)


if __name__ == "__main__":
    refinement_summary(occ_conv_csv="/dls/science/groups/i04-1/elliot-dev/"
                                      "Work/exhaustive_parse_xchem_db/occ_conv.csv",
                       refine_csv="/dls/science/groups/i04-1/elliot-dev/"
                                      "Work/exhaustive_parse_xchem_db/refinement.csv",
                       superposed_csv="/dls/science/groups/i04-1/elliot-dev/"
                                  "Work/exhaustive_parse_xchem_db/superposed.csv",
                       log_pdb_mtz_csv="/dls/science/groups/i04-1/elliot-dev/"
                                  "Work/exhaustive_parse_xchem_db/log_pdb_mtz.csv",
                       out_csv="/dls/science/groups/i04-1/elliot-dev/"
                                  "Work/exhaustive_parse_xchem_db/refinement_summary.csv")