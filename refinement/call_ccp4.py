import ast
import os


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
    lig_pos: list
        A list of tuples for chain, and resname, resid.
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

    return lig_pos
