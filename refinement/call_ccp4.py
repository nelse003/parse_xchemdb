import ast
import os

from refinement.parameters import lig_pos_to_occupancy_refinement_string


def get_incomplete_occ_groups(tmp_dir, crystal, pdb, script_dir, ccp4_path):
    """
    Get occupancy groups from pdb by calling ligand.py

    Parameters
    ----------
    tmp_dir: str
        path to directory where tmp files will be written
    crystal: str
        crystal name
    pdb: str
        path to pdb file to be parsed
    ccp4_path: str
        path to source ccp4 distribution from

    Returns
    -------
    occ_group: str
        A string to be appended to the refinment csh,
        containing occupancy groups incomplete for each residue
        which is thought to be a ligand of interest

    """

    # Read pdb and determine ligand occupancy groups
    tmp_file = os.path.join(tmp_dir, "lig_pos_tmp_{}.txt".format(crystal))
    source = "source {}".format(ccp4_path)
    os.system(source)
    ligand_py = os.path.join(script_dir, "ccp4/ligand.py")
    os.system("ccp4-python {} {} {}".format(ligand_py, pdb, tmp_file))
    with open(tmp_file) as f:
        lig_pos = f.read()
    os.remove(tmp_file)

    # Convert string in file to list
    lig_pos = ast.literal_eval(lig_pos)
    occ_group = lig_pos_to_occupancy_refinement_string(list(lig_pos))

    return occ_group
