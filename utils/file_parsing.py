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
