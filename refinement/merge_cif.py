import os, glob

def merge_cif(cif1, cif2, cif_out):

    """
    Merge cif using libcheck

    Parameters
    ----------
    cif1: str
        path to inpuit cif 1
    cif2: str
        path to input cif 2
    cif_out: str
        path to output cif

    Returns
    -------
    None

    """

    cmd = (
        "#!/bin/bash\n"
        "\n"
        "$CCP4/bin/libcheck << eof \n"
        "_Y\n"
        "_FILE_L " + cif1 + "\n"
        "_FILE_L2 " + cif2 + "\n"
        "_FILE_O " + cif_out + "\n"
        "_END\n"
        "eof\n"
    )
    print(cmd)
    os.system(cmd)
