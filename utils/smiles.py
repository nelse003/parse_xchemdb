import os
import shutil
import subprocess

from parse_xchemdb import parse_args, get_databases, get_table_df


def smiles_to_cif_acedrg(smiles, out_file):
    """
    Run Acedrg with smiles to generate cif

    Parameters
    ----------
    smiles: str
        smiles string to be passed to acedrg
    out_file: str
        path to output cif file

    Returns
    -------
    None

    Notes
    -------
    Will generate cif file, and remove intermediary _TMP folder

    Raises
    --------
    FileNotFoundError
        If acedrg fail accorsing to specified error string:
        "Can not generate a molecule from the input SMILES string!"
        or if the output file
    """

    # Acedrg adds .cif extension,
    # so if it is in the orginbal file name remove it
    if ".cif" in out_file:
        out_file = out_file.split(".cif")[0]

    # Wrapping a basic command line implementation of acedrg
    a = subprocess.run(
        ["acedrg", "-i", smiles, "-o", out_file],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
    )

    # Error specific to cif file not being produced
    smiles_error = "Can not generate a molecule from the input SMILES string!"

    # Check the stdout of acedrg for the error
    if smiles_error in a.stdout:
        raise FileNotFoundError(
            "{}.cif cannot be produced from the smiles using acedrg"
        )

    # Check whether there is a output file at all
    if not os.path.isfile(out_file):
        raise FileNotFoundError("{}.cif was not produced from the smiles using acedrg")

    # specify temporary folder name
    tmp_folder = input_cif + "_TMP"

    # remove intermediary _TMP folder
    if os.path.isdir(tmp_folder):
        shutil.rmtree(tmp_folder)


def smiles_from_crystal(crystal):

    """
    Get smiles string from crystal_name in XChemDB postgres tables

    Parameters
    ----------
    crystal: str
        crystal name

    Returns
    -------
    smiles: str
        smiles string

    Notes
    -----
    This parses the XChemDB postgres table to multiple dataframes
    just to read one line. This will be inefficent,
    but is currently only being called a few times currently
    """

    args = parse_args()
    databases = get_databases(args)

    compounds = get_table_df(table_name="compounds", databases=databases, args=args)

    crystals = get_table_df(table_name="crystals", databases=databases, args=args)

    # Use query to get cmpd_id from crystal table
    cmpd_id = crystals.query("crystal_name==@crystal")["compound_id"].values[0]

    # Use query to get smiles from cmpd_id
    smiles = compounds.query("id==@cmpd_id")["smiles"].values[0]

    return smiles
