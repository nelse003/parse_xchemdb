import os


def split_conformations(pdb, working_dir=None):
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


def make_restraints(pdb, ccp4, refinement_program, working_dir=None):

    source = "source {}".format(ccp4)
    os.system(source)

    if not os.path.isdir(working_dir):
        os.makedirs(working_dir)

    if working_dir is not None:
        os.chdir(working_dir)

    os.system("giant.make_restraints {}".format(pdb))

    link_pdb = os.path.join(working_dir, "input.link.pdb")
    new_refmac_restraints = os.path.join(
        working_dir, "multi-state-restraints.refmac.params"
    )
    new_phenix_restraints = os.path.join(
        working_dir, "multi-state-restraints.phenix.params"
    )

    if refinement_program == "refmac":

        if os.path.isfile(new_refmac_restraints):
            input_params = new_refmac_restraints

        if os.path.isfile(link_pdb):
            pdb = link_pdb

    if refinement_program == "phenix":
        if os.path.isfile(new_phenix_restraints):
            input_params = new_phenix_restraints

    return input_params, pdb
