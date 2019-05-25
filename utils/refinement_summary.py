import pandas as pd


def refinement_summary(
    occ_state_comment_csv, refine_csv, superposed_csv, log_pdb_mtz_csv, out_csv
):
    """
    Summarise refinement information for plotting

    Parameters
    ----------
    occ_state_comment_csv: str
        path to occupancy convergence csv
    refine_csv: str
        path to csv representing refinement table
    superposed_csv: str
        path to csv representing refinement table
        with rows with non-existence pdb's removed
    log_pdb_mtz_csv: str
        path to refinement table with crystal information
    out_csv: str
        path to output csv

    Returns
    -------
    None
    """

    # Read in csvs
    occ_conv_df = pd.read_csv(occ_state_comment_csv)
    refine_df = pd.read_csv(refine_csv)
    superposed_df = pd.read_csv(superposed_csv)
    log_pdb_mtz_df = pd.read_csv(log_pdb_mtz_csv)

    # Remove the rows in refinemnt table with pdb_latest missing
    pdb_df = refine_df[refine_df.pdb_latest.notnull()]

    # Pull out comments from occupancy convergence csv
    comments = occ_conv_df[["comment", "crystal", "complete group"]]

    # Drop duplicates to get unique rows,
    # this reduces down to one row per complete group
    comments = comments.drop_duplicates()

    # Get number of crystals with coment
    summary = comments["comment"].value_counts()

    # Make dict to add in further details to summary df
    summary_dict = {}
    summary_dict["Refinement latest is dimple"] = len(pdb_df) - len(superposed_df)
    summary_dict["Refinement log is missing"] = len(superposed_df) - len(log_pdb_mtz_df)

    # Append dict to summary df
    summary = summary.append(pd.Series(summary_dict))

    # Write to csv
    summary.to_csv(out_csv, header=False)
