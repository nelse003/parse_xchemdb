import os
from pathlib import Path

def split_conformations(pdb, working_dir=None, refinement_type="bound"):
    """Run giant.split conformations preserving occupancies

    Parameters
    ----------
    pdb: str
        path to input pdb with merged conformations

    """

    if not os.path.isdir(working_dir):
        os.makedirs(working_dir)

    if working_dir is not None:
        os.chdir(working_dir)

    cmd = (
        "giant.split_conformations"
        + " input.pdb='{}'".format(pdb)
        + " reset_occupancies=False"
        + " suffix_prefix=split"
    )
    os.system(cmd)

    split_output = os.path.join(working_dir,
        "{}.split.{}-state.pdb".format(os.path.basename(pdb).strip(".pdb"),
                                                      refinement_type))
    p=Path(pdb)
    p.unlink()
    os.symlink(src=split_output, dst=pdb)

def make_restraints(pdb, ccp4, refinement_program, working_dir=None):

    if not os.path.isdir(working_dir):
        os.makedirs(working_dir)

    if working_dir is not None:
        os.chdir(working_dir)

    os.system("source {};giant.make_restraints {}".format(ccp4, pdb))

    link_pdb = os.path.join(working_dir, "input.link.pdb")
    new_refmac_restraints = os.path.join(
        working_dir, "multi-state-restraints.refmac.params"
    )
    new_phenix_restraints = os.path.join(
        working_dir, "multi-state-restraints.phenix.params"
    )
    new_buster_restraints = os.path.join(working_dir, "params.gelly")

    if refinement_program == "refmac":

        if os.path.isfile(new_refmac_restraints):
            input_params = new_refmac_restraints

        if os.path.isfile(link_pdb):
            pdb = link_pdb

    elif refinement_program == "phenix":
        if os.path.isfile(new_phenix_restraints):
            input_params = new_phenix_restraints

    elif refinement_program == "buster":
        if os.path.isfile(new_buster_restraints):
            input_params = new_buster_restraints

    elif refinement_program == "exhaustive":
        input_params = None

    return input_params, pdb
