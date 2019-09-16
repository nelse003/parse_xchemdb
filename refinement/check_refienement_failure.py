from file_parsing import check_file_for_string
from filesystem import get_most_recent_quick_refine


def check_restraints(input_dir):
    """
    Check if quick-refine fails due restraint mismatch

    Error checked in quick-refine log
    "Error: At least one of the atoms from the restraints could not be found"

    Parameters
    ----------
    input_dir: str
        path to the refinement directory which should
        contain subfolders with quick-refine logs

    Returns
    -------
    Bool:
        True if the error is associated with restraints
        False if error is not in quick-refine file,
        or refinement has not yet occured in this folder

    """
    try:
        quick_refine_log = get_most_recent_quick_refine(input_dir)
    except FileNotFoundError:
        return False

    if check_file_for_string(
        quick_refine_log,
        "Error: At least one of the atoms from" " the restraints could not be found",
    ):
        return True


def check_refinement_for_cif_error(input_dir):
    """
    Check whether the refinement has failed due to a cif error

    Error checked in quick-refine log
    "Refmac:  New ligand has been encountered. Stopping now"

    Parameters
    ----------
    input_dir: str
        path to refinement folder

    Returns
    -------
        Bool:
        True if the error is asscoiated with cif file
        False if error is not in quick-refine file,
        or refinement has not yet occured in this folder
    """

    # Need to catch file not found error,
    # as perhaps an existing refinement doesn't exist
    # in current folder.
    try:
        quick_refine_log = get_most_recent_quick_refine(input_dir)
    except FileNotFoundError:
        return False

    if check_file_for_string(
        quick_refine_log, "Refmac:  New ligand has been encountered. Stopping now"
    ):
        return True

    # phenix error, could be fixed with new cif.
    # First found in HAO1A-x0592
    if check_file_for_string(quick_refine_log, "KeyError: 'HCR'"):
        return True

    # phenix error, Found in PaFen_c3-x0443.
    # Might be cif related, orignally assumed
    # to be an issue with BP3 not being defined
    # in monomer library, but this is not the case.
    # It is unlikely just a LIG cif will fix,
    # but should be tried before more complex solutions enacted

    if check_file_for_string(
        quick_refine_log, "Sorry: Fatal problems interpreting model file"
    ):
        return True

    if check_file_for_string(
        quick_refine_log, "unable to find a dictionary for residue"
    ):
        print("BUSTER ASJJA")
        exit()
