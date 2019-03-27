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

    cmd =(
    'giant.split_conformations' +
    " input.pdb='{}'".format(pdb) +
    ' reset_occupancies=False' +
    ' suffix_prefix=split'
    )
    os.system(cmd)