import pandas as pd
import numpy as np
from iotbx.pdb import hierarchy
import argparse

def update_from_pdb(pdb_df):

    pdb = pdb_df.pdb_latest.unique()[0]

    pdb_in = hierarchy.input(file_name=pdb)
    sel_cache = pdb_in.hierarchy.atom_selection_cache()

    res_df = pdb_df[['resid','chain','alte']]
    rows = []
    for index, row in pdb_df.iterrows():

        sel = sel_cache.selection("chain {} resid {} altloc {}".format(row.chain,
                                                                       row.resid,
                                                                       row.alte))
        hier = pdb_in.hierarchy.select(sel)
        resnames = []
        for chain in hier.only_model().chains():
            for residue_group in chain.residue_groups():
                for atom_group in residue_group.atom_groups():
                    resnames.append(atom_group.resname)
                    b = []
                    for atom in atom_group.atoms():
                        b.append(atom.b)

                    mean_b = np.mean(b)
                    std_b  = np.std(b)


        if len(resnames) == 1:
            row['resname'] = resnames[0]
            row['B_mean'] = mean_b
            row['B_std'] = std_b
            rows.append(row)
        else:
            raise ValueError("Multiple residues for "
                             "chain {} resid {} altloc {} "
                             "of pdb: {}".format(chain, resid, alte, pdb))


    return pd.concat(rows, axis=1)



def get_resname_for_log_occ(log_occ_csv, log_occ_resname_csv):

    log_df = pd.read_csv(log_occ_csv)

    log_df_list = []
    for pdb in log_df.pdb_latest.unique():
        pdb_df = log_df.loc[log_df['pdb_latest'] == pdb]
        pdb_df = update_from_pdb(pdb_df)
        print(pdb_df)
        print(pdb_df.columns)
        break
        log_df_list.append(pdb_df)

    raise Exception

    log_occ_resname_df = pd.concat(log_df_list)
    log_occ_resname_df.to_csv(log_occ_resname_csv)

def main():


    parser = argparse.ArgumentParser()
    parser.add_argument("log_occ_csv",
                        help="input csv with log info",
                        type=str)
    parser.add_argument("log_occ_resname_csv",
                        help="output csv with log info and resnames",
                        type=str)

    args = parser.parse_args()
    get_resname_for_log_occ(args.log_occ_csv, args.log_occ_resname_csv)

if __name__ == "__main__":
    main()