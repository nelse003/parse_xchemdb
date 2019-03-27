from utils.file_parsing import check_file_for_string
from utils.filesystem import get_most_recent_quick_refine


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
        True if the error is
        False if error is not in quick-refine file,
        or refinement has not yet occured in this folder

    """
    try:
        quick_refine_log = get_most_recent_quick_refine(input_dir)
    except FileNotFoundError:
        return False
    return check_file_for_string(quick_refine_log,
                                 "Error: At least one of the atoms from"
                                 " the restraints could not be found")


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
        True if the error is
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

    return check_file_for_string(quick_refine_log,
                                 "Refmac:  New ligand has been encountered. Stopping now")