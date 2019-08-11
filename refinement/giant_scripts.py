import os
from pathlib import Path


def split_conformations(pdb, working_dir=None, refinement_type="bound"):
    """Run giant.split conformations changing occupancies

    Parameters
    ----------
    pdb: str
        path to input pdb with merged conformations

    """

    if not os.path.isdir(working_dir):
        os.makedirs(working_dir)

    if working_dir is not None:
        os.chdir(working_dir)

    bound_state_pdb = os.path.join(
        working_dir,
        "{}.split.{}-state.pdb".format(os.path.basename(pdb).strip(".pdb"), "bound"),
    )
    ground_state_pdb = os.path.join(
        working_dir,
        "{}.split.{}-state.pdb".format(os.path.basename(pdb).strip(".pdb"), "ground"),
    )
    # Split conformations
    cmd = (
        "giant.split_conformations"
        + " input.pdb='{}'".format(pdb)
        + " reset_occupancies=False"
        + " suffix_prefix=split"
    )
    os.system(cmd)

    # merge conformations changing occupancy
    cmd = (
        "giant.merge_conformations"
        + " input.major={}".format(ground_state_pdb)
        + " input.minor={}".format(bound_state_pdb)
        + " options.minor_occupancy=0.9"
    )
    os.system(cmd)

    # Remove original split conformations
    if os.path.exists(ground_state_pdb):
        os.remove(ground_state_pdb)

    if os.path.exists(bound_state_pdb):
        os.remove(bound_state_pdb)

    p = Path(pdb)
    p.unlink()
    os.symlink(
        src=os.path.join(os.path.dirname(ground_state_pdb), "multi-state-model.pdb"),
        dst=pdb,
    )

    # Split merged conformations
    cmd = (
        "giant.split_conformations"
        + " input.pdb='{}'".format(pdb)
        + " reset_occupancies=False"
        + " suffix_prefix=split"
    )
    os.system(cmd)

    split_output = os.path.join(
        working_dir,
        "{}.split.{}-state.pdb".format(
            os.path.basename(pdb).strip(".pdb"), refinement_type
        ),
    )

    p = Path(pdb)
    p.unlink()
    os.symlink(src=split_output, dst=pdb)


def make_restraints(pdb, ccp4, refinement_program, working_dir=None):

    if not os.path.isdir(working_dir):
        os.makedirs(working_dir)

    if working_dir is not None:
        os.chdir(working_dir)

    link_pdb = os.path.join(working_dir, "input.link.pdb")
    new_refmac_restraints = os.path.join(
        working_dir, "multi-state-restraints.refmac.params"
    )
    new_phenix_restraints = os.path.join(
        working_dir, "multi-state-restraints.phenix.params"
    )
    new_buster_restraints = os.path.join(working_dir, "params.gelly")

    if refinement_program == "refmac":
        input_params = new_refmac_restraints
        if os.path.isfile(link_pdb):
            pdb = link_pdb

    elif refinement_program == "phenix":
        input_params = new_phenix_restraints

    elif refinement_program == "buster":
        input_params = new_buster_restraints

    elif refinement_program == "exhaustive":
        input_params = None

    if input_params is not None:
        if not os.path.exists(input_params):
            os.system("source {};" "giant.make_restraints {}".format(ccp4, pdb))

    return input_params, pdb
