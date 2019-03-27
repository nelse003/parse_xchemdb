import os


def make_symlink(file, link_dir, link_name):
    """ Generate symlink

    Parameters
    -----------
    file: str
        path to file to make link
    link_dir: str
        directory to symlink
    link_name: str
        name of symlink file
    """
    if file is not None:
        link = os.path.join(link_dir, link_name)
        if not os.path.exists(link):
            os.symlink(file, link)
    else:
        link = None

    return link


def make_symlinks(input_dir, cif, pdb, params, free_mtz):
    """
    Make symbolic links to refinement input files

    Parameters
    ----------
    input_dir: str
        path to input directory
    cif: str
        path to cif file
    pdb:
        path to pdb file
    params:
        path to parameter file
    free_mtz:
        path to free mtz

    Returns
    -------
    input_cif: str
        path to cif file for refinement
    input_pdb: str
        path to pdb file for refinement
    input_params: str
        path to parameter file for refinement
    input_mtz: str
        path to mtz file for refinement

    """

    # Generate symlinks if they do not exist
    input_cif = make_symlink(file=cif,
                             link_dir=input_dir,
                             link_name="input.cif")

    input_pdb = make_symlink(file=pdb,
                             link_dir=input_dir,
                             link_name="input.pdb")

    input_params = make_symlink(file=params,
                             link_dir=input_dir,
                             link_name="input.params")

    input_mtz = make_symlink(file=free_mtz,
                             link_dir=input_dir,
                             link_name="input.mtz")

    return input_cif, input_pdb, input_params, input_mtz