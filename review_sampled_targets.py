import pandas as pd
import os

out_dir= "/dls/science/groups/i04-1/elliot-dev/Work/exhaustive_parse_xchem_db/test_sample_all_targets_30_07_19/"

target_df = pd.read_csv("test.csv")

crystals = target_df.crystal_name.values

folders = {'buster':'refine.pdb',
           'buster_superposed':'refine.pdb',
           'phenix':'refine_001.pdb',
           'phenix_superposed':'refine_0001/refine_1_001.pdb',
           'exhaustive':'exhaustive_search.csv',
           'convergence_refinement': 'refine.pdb',
           'bound_refinement':'refine.pdb'}

for folder, refine_name in folders.items():
    check_dict = {}
    for crystal in crystals:
        check_refine_file = os.path.join(out_dir, folder, crystal, refine_name)
        check_dict[crystal] = os.path.exists(check_refine_file)

    target_df[folder] = target_df['crystal_name'].map(check_dict)

target_df.to_csv('sample_all_targets.csv')