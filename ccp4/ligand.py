import argparse
from iotbx.pdb import hierarchy


def get_lig(pdb):

    lig_names = ["LIG", "UNL", "DRG"]

    # read into iotbx.hierarchy
    pdb_in = hierarchy.input(file_name=pdb)
    # read into iotbx.selection cache
    sel_cache = pdb_in.hierarchy.atom_selection_cache()

    lig_pos = []
    for lig in lig_names:
        sel = sel_cache.selection("resname {}".format(lig))
        hier = pdb_in.hierarchy.select(sel)

        if hier.models_size() == 0:
            continue

        for chain in hier.only_model().chains():
            for residue_group in chain.residue_groups():
                for atom_group in residue_group.atom_groups():
                    chain_id = chain.id
                    resname = atom_group.resname
                    resseq = str(int(residue_group.resseq))
                    lig_pos.append((chain_id, resname, resseq))

    return lig_pos


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("pdb", help="path to pdb file", type=str)
    parser.add_argument("tmp_file", help="temporary file to parse output", type=str)
    args = parser.parse_args()

    with open(args.tmp_file, "w") as f:
        f.write(str(get_lig(args.pdb)))
