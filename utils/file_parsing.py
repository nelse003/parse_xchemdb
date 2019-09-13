import os

def pdb_file_to_crystal(file):

    """
    Turns path with folder named of crystal into crystal_name

    Parameters
    ----------
    file

    Returns
    -------

    """

    return os.path.basename(os.path.dirname(file))

def strip_bdc_file_path(file):
    """
    Turn event maps into file path into 1-BDC

    Parameters
    ----------
    file

    Returns
    -------

    """
    return os.path.basename(file).split('BDC')[1].split('_')[1]


def check_file_for_string(file, string):
    """Check if string is in file

    Parameters
    ------------
    file: str
        path to file
    string: str
        string to be checked for existence in file

    Returns
    --------
    Bool:
        True if string in file, False otherwise

    Notes
    -----------
    Reads entire file into memory, so would work for very large files
    """
    file_txt = open(file, "r").read()
    if string in file_txt:
        return True
    else:
        return False
