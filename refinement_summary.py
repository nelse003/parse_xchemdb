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
