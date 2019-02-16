import pandas as pd

def check_str(s, start):
    if s.startswith(start):
        return start
    else:
        return s

def refinement_summary(occ_conv_csv,
                       refine_csv,
                       superposed_csv,
                       occ_conv_failures_csv,
                       log_pdb_mtz_csv,
                       out_csv):

    occ_conv_df = pd.read_csv(occ_conv_csv)
    refine_df = pd.read_csv(refine_csv)
    superposed_df = pd.read_csv(superposed_csv)
    log_pdb_mtz_df = pd.read_csv(log_pdb_mtz_csv)
    occ_conv_failures_df = pd.read_csv(occ_conv_failures_csv)

    pdb_df = refine_df[refine_df.pdb_latest.notnull()]


    # Strip to variable protion of similair errors
    occ_conv_failures_df['exception'] = \
        occ_conv_failures_df['exception'].apply(
            check_str, start=("Failing to parse crystal"))

    occ_conv_failures_df['exception'] = \
        occ_conv_failures_df['exception'].apply(
            check_str, args=("Ligand chain is not unique",))

    failures_summary = occ_conv_failures_df.groupby('exception').count()['log']

    # division by 2 as ground and bound
    summary = occ_conv_df.groupby('comment').count()['crystal']/2

    summary_dict = {}
    summary_dict['Refinement latest is dimple'] = len(pdb_df)-len(superposed_df)
    summary_dict['Refinement log is missing'] = len(superposed_df) - len(log_pdb_mtz_df)

    summary = summary.append(pd.Series(summary_dict))
    summary = summary.append(failures_summary)
    summary.to_csv(out_csv, header=False)


if __name__ == "__main__":
    refinement_summary(occ_conv_csv="/dls/science/groups/i04-1/elliot-dev/"
                                      "Work/exhaustive_parse_xchem_db/occ_conv.csv",
                       refine_csv="/dls/science/groups/i04-1/elliot-dev/"
                                      "Work/exhaustive_parse_xchem_db/refinement.csv",
                       occ_conv_failures_csv= "/dls/science/groups/i04-1/elliot-dev/"
                                      "Work/exhaustive_parse_xchem_db/occ_conv_failures.csv",
                       superposed_csv="/dls/science/groups/i04-1/elliot-dev/"
                                  "Work/exhaustive_parse_xchem_db/superposed.csv",
                       log_pdb_mtz_csv="/dls/science/groups/i04-1/elliot-dev/"
                                  "Work/exhaustive_parse_xchem_db/log_pdb_mtz.csv",
                       out_csv="/dls/science/groups/i04-1/elliot-dev/"
                                  "Work/exhaustive_parse_xchem_db/refinement_summary.csv")