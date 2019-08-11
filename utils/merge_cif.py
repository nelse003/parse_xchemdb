import os, glob

cif = "/dls/labxchem/data/2017/lb18145-17/processing/reference/XX02KALRNA-Rac1-Dk-2-reference.cif"


def merge_cif(cif1, cif2, cif_out):

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
