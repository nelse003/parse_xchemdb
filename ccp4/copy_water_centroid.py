import os
import argparse
from iotbx.pdb import hierarchy
from scitbx.array_family import flex

if __name__ =="__main__":

    """
    Copy a water atom into the centroid of ligand.
    """

    # parse path top ground and bound pdb
    parser = argparse.ArgumentParser('copy water atom to ligand centroid')
    parser.add_argument('--bound_pdb')
    parser.add_argument('--ground_pdb')
    parser.add_argument('--output_pdb')
    param = parser.parse_args()

    # Get centroid of ligand from bound pdb
    bound_pdb_in = hierarchy.input(file_name=param.bound_pdb)
    bound_sel_cache = bound_pdb_in.hierarchy.atom_selection_cache()
    selection_string = "resname LIG"
    lig_sel = bound_sel_cache.selection(selection_string)
    lig_hier = bound_pdb_in.hierarchy.select(lig_sel)
    lig_centroid =lig_hier.atoms().extract_xyz().mean()

    # read in ground state pdb
    ground_pdb_in = hierarchy.input(file_name=param.ground_pdb)
    ground_sel_cache = ground_pdb_in.hierarchy.atom_selection_cache()

    # get water selection
    wat_sel = bound_sel_cache.selection("water")
    wat_hier = bound_pdb_in.hierarchy.select(wat_sel)

    wat_resseq = wat_hier.atoms()[-1].parent().parent().resseq
    wat_chain = wat_hier.atoms()[-1].parent().parent().parent().id

    last_wat_copy  = wat_hier.atoms()[-1].detached_copy()
    last_wat_copy.set_xyz(lig_centroid)


    #ground_pdb_in.hierarchy.overall_counts().show()

    # get atom and residue groups to copy last_wat into
    for chain in ground_pdb_in.hierarchy.only_model().chains():
        if chain.id != wat_chain:
            continue
        for residue_group in chain.residue_groups():
            if residue_group.resseq != wat_resseq:
                continue
            rg = residue_group.detached_copy()
            for atom_group in residue_group.atom_groups():
                ag = atom_group.detached_copy()
                break

    # Can't seem to reset iseq?
    # # get max i_seq
    # max_i_seq=0
    # for atom in ground_pdb_in.hierarchy.only_model().atoms():
    #     if atom.i_seq > max_i_seq:
    #         max_i_seq = atom.i_seq
    # set_i_seq
    #last_wat_copy.i_seq = max_i_seq

    # remove existing atoms in group
    ag.remove_atom(0)
    # Add atom to atom group
    ag.append_atom_with_other_parent(last_wat_copy)
    #change the resid
    rg.resseq = int(wat_resseq) + 1
    # remove existing atom group
    rg.remove_atom_group(0)
    # add new atoms group
    rg.append_atom_group(ag)

    # Add atom to hierarchy
    for chain in ground_pdb_in.hierarchy.only_model().chains():
        if chain.id != wat_chain:
            continue
        chain.append_residue_group(rg)
        break


    with open(param.output_pdb, 'w') as f:
        f.write(ground_pdb_in.hierarchy.as_pdb_string(
            crystal_symmetry=
            bound_pdb_in.crystal_symmetry()))

