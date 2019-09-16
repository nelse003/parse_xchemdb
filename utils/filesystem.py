import glob
import os
import shutil
import pandas as pd

from smiles import smiles_from_crystal
from smiles import smiles_to_cif_acedrg
from refinement.giant_scripts import make_restraints

from path_config import Path


def parse_refinement_folder(refinement_dir, refinement_csv, refinement_type, test=None):

    """
    Parse folder for refinements

    Parse a refinement folder to get a csv with minimally:
        refine_log: path to refinement log
        crystal_name: crystal name
        pdb_latest: path to
        mtz_latest: path to latest mtz

    The folder structure to be parsed is:
        ./<crystal>/refine.pdb
        ./<crystal>/refine.mtz
        ./<crystal>/refmac.log or ./<crystal>/refine_XXXX/*quick.refine.log

    Parameters
    ----------
    refinement_dir: str
        path to refienement directory
    refinement_csv: str
        path to refinement csv file
    refinement_type: str
        "superposed" or "bound"

    Returns
    -------
    None
    """

    pdb_mtz_log_dict = {}

    # Loop over folders
    for crystal in os.listdir(refinement_dir):

        pdb_latest = None
        mtz_latest = None
        refinement_log = None

        crystal_dir = os.path.join(refinement_dir, crystal)

        if not os.path.isdir(crystal_dir):
            continue

        # Check for existence of refine.pdb and refine.mtz
        for f in os.listdir(crystal_dir):

            if f == "refine.pdb":
                pdb_latest = os.path.join(crystal_dir, f)
            elif f == "refine.mtz":
                mtz_latest = os.path.join(crystal_dir, f)

        # When giant.quick.refine has been used
        if refinement_type == "superposed":
            try:
                refinement_log = get_most_recent_quick_refine(crystal_dir)
            except FileNotFoundError:
                continue

        # When refmac refinement of bound state has been performed
        elif refinement_type == "bound":
            for f in os.listdir(crystal_dir):
                if f == "refmac.log":
                    refinement_log = os.path.join(refinement_dir, crystal, f)
                elif f == "refine.log":
                    refinement_log = os.path.join(refinement_dir, crystal, f)

        # Add row if pdb, mtz and log have been found for this crystal
        if None not in [pdb_latest, mtz_latest, refinement_log]:
            pdb_mtz_log_dict[crystal] = (pdb_latest, mtz_latest, refinement_log)

    # Write dictionary of pdb, mtz and logs to dataframe
    df = pd.DataFrame.from_dict(
        data=pdb_mtz_log_dict,
        columns=["pdb_latest", "mtz_latest", "refine_log"],
        orient="index",
    )
    # Name the index
    df.index.name = "crystal_name"
    # Write dataframe to csv file
    df.to_csv(refinement_csv)


def find_program_from_parameter_file(file):
    """ Read parameter file to determine whether refmac or phenix

    May not be sufficient for all files,
    as checks may miss some parts of file

    Parameters
    ----------
    file: str
        file path to checked

    Returns
    ----------
    str
        refinement program if identifiable from input file
    None
        If refinment program cannot be identified
    """

    file_txt = open(file, "r").read()

    if "occupancy group id" in file_txt:
        return "refmac"
    elif "refinement.geometry_restraints.edits" in file_txt:
        return "phenix"
    else:
        return None


def parameter_from_refine_pdb(pdb, glob_string, refinement_program):

    """
    Search recursively for a parameter file

    If a single parameter file is found in path of pdb,
    then that is used. Else check for correct refinement
    program from list of multiple files

    Parameters
    ----------
    pdb: str
        path to pdb location to start recursive search
    glob_string: str
        string to match, using glob.
        Include any required glob expressions
    refinement_program: str
        refinement program; phenix or refmac

    Returns
    -------
    file: str
        path to parameter file matching string
    None
        If the recursive search is unsuccessful
    """

    path = os.path.dirname(pdb)
    files_list = glob.glob(os.path.join(path, glob_string))

    # If only a single parameter file is present use that
    for f in files_list:
        if find_program_from_parameter_file(f) == refinement_program:
            if len(files_list) == 1:
                return files_list[0]

            # If multiple paramter files are present
            elif len(files_list) >= 1:

                # If the name of the refinement program
                # appears in the file string
                if refinement_program in f:
                    return f

    if path == "/":
        return

    return parameter_from_refine_pdb(
        path, glob_string=glob_string, refinement_program=refinement_program
    )


def path_from_refine_pdb(pdb, glob_string):
    """
    Recursively search pdb path for files matching the glob string

    Parameters
    ----------
    pdb: str
        path to pdb
    glob_string: str
        string to match, using glob.
        Include any required glob expressions

    Returns
    -------
    str
        path to file matching glob_string
    None
        If the recursive search is unsuccessful
    """
    path = os.path.dirname(pdb)
    files_list = glob.glob(os.path.join(path, glob_string))

    if len(files_list) >= 1:
        return files_list[0]
    elif path == "/":
        return

    return path_from_refine_pdb(path, glob_string=glob_string)


def free_mtz_path_from_refine_pdb(pdb):
    """
    Search recursively for free mtz file

    Parameters
    ----------
    pdb: str
        path to pdb file

    Returns
    -------
    str
        path to free mtz

    """
    return path_from_refine_pdb(pdb, glob_string="*.free.mtz")


def cif_path(cif="", pdb="", input_dir=None, crystal=None):
    """
    Checks cif file existence, recursive search or regenerate from smiles

    First checks whether cif is provided and exists,
    next search implicitly in the pdb location.

    Parameters
    ----------
    cif
    pdb
    input_dir
    crystal

    Returns
    -------
    cif_p: str
        path to cif file

    Raises
    --------
    FileNotFoundError
        If cif file cannot be found or regenerated

    """
    cif_p = None

    if cif is not "":
        # Check file existence
        if os.path.exists(cif):
            cif_p = cif
        elif pdb is not "":
            cif = os.path.basename(cif)
            # Check for named cif file implicit in the pdb location
            cif_p = path_from_refine_pdb(pdb=pdb, glob_string=cif)
        else:
            raise FileNotFoundError(
                "Cif path cannot be found, " "try providing pdb path"
            )
    else:
        # check for any cif implicit in the pdb location
        cif_p = path_from_refine_pdb(pdb, glob_string="*.cif")

    # Try regenerating cif using smiles
    if cif_p is None:
        input_cif = os.path.join(input_dir, "input.cif")
        smiles = smiles_from_crystal(crystal)
        smiles_to_cif_acedrg(smiles=smiles, out_file=input_cif)
        cif_p = input_cif

    # now run last as may be possible to regenerate cif if pdb path is not present
    if cif_p is "" and pdb is "":
        raise FileNotFoundError("Path and cif cannot both be None")

    return cif_p


def check_inputs(
    cif,
    pdb,
    params,
    free_mtz,
    refinement_program,
    input_dir,
    crystal,
    out_dir,
    ccp4=Path().ccp4,
):
    """
    Check whether refinement input files are valid, replace if not and possible

    If cif file path does not exist search the pdb
    file path for a cif file. If this fails,
    check database for a smiles string, then run acedrg.
    pdb path is just checked for existence.
    If the mtz file, and parameter file path
    does not exist, search implicitly at pdb file path.

    Parameters
    ----------
    ccp4
    cif: str
        path to cif file
    pdb: str
        path to pdb file
    params: str
        path to refinemnt parameter file
    free_mtz: str
        path to free mtz file
    refinement_program: str
        name of refinement program; refmac or phenix
    input_dir: str
        path to directory at which input files are located
    crystal: str
        name of crystal

    Returns
    -------
    cif: str
        path to cif file, adjusted if input file did not exist
    params: str
        path to parameter file, adjusted if parameter input
        file did not exist
    free_mtz: str
        path to mtz, adjusted if file does nto exist

    Raises
    -------
    FileNotFoundError:
        If cif, pdb, mtz or parameter files do not exist after
        trying alternative location/ regenerating in the case of cif file
    """

    if not os.path.exists(input_dir):
        os.makedirs(input_dir)

    # If Cif file is not found at supplied location (due to error in database),
    # or it is not supplied, it's implicit location:
    # (same folder as refine.pdb symlink) is checked.
    # If that doesn't work try generating from smiles string

    cif = cif_path(cif=cif, pdb=pdb, input_dir=input_dir, crystal=crystal)

    # If pdb file does not exist raise error
    if not os.path.isfile(pdb):
        raise FileNotFoundError("{}: pdb Not found".format(pdb))

    # Check that free_mtz is provided.
    # An empty string is used rather than None,
    # due to luigi.Parameters passing strings around.
    # If it doesn't exist try to infer location relative
    # to the provided pdb.
    if free_mtz == "":
        free_mtz = free_mtz_path_from_refine_pdb(pdb)

    # Get free mtz recursively if possible,
    # else get refine mtz file recursively,
    # or the orignal non-free mtz
    if free_mtz == None or not os.path.isfile(free_mtz):
        free_mtz = free_mtz_path_from_refine_pdb(pdb)
    if free_mtz == None or not os.path.isfile(free_mtz):
        free_mtz = path_from_refine_pdb(pdb, "refine*.mtz")
    if free_mtz == None or not os.path.isfile(free_mtz):
        free_mtz = path_from_refine_pdb(pdb, "{}*.mtz".format(crystal))

    # If parameter file is not provided,
    # search the relative_path for the pdb file
    # First look for a file in the folder
    if params is "" or params is None:
        params = parameter_from_refine_pdb(
            pdb, glob_string="*params", refinement_program=refinement_program
        )

    if params is None:
        params, _ = make_restraints(
            pdb=pdb,
            ccp4=ccp4,
            refinement_program=refinement_program,
            working_dir=input_dir,
        )

    # If the refinement program does not match the parameter file,
    # search for one that does in relative_path for the pdb file
    if refinement_program != "exhaustive":
        if find_program_from_parameter_file(params) != refinement_program:
            params = parameter_from_refine_pdb(
                pdb, glob_string="*params", refinement_program=refinement_program
            )
    if refinement_program != "exhaustive":
        if params is None:
            params, _ = make_restraints(
                pdb=pdb,
                ccp4=ccp4,
                refinement_program=refinement_program,
                working_dir=input_dir,
            )

    # Check that the source files for the symlinks exist
    if not os.path.isfile(cif):
        raise FileNotFoundError("{}: cif Not found".format(cif))

    if refinement_program != "exhaustive":
        if not os.path.isfile(params):
            raise FileNotFoundError("{}: parameter file Not found".format(params))

    if not os.path.isfile(free_mtz):
        raise FileNotFoundError("{}: mtz Not found".format(free_mtz))

    if refinement_program == "buster" and "PDK2" in crystal:
        os.system(
            "module load phenix;"
            "phenix.ready_set "
            "input.pdb_file_name={} "
            "input.output_dir={}".format(pdb, os.path.join(out_dir, crystal))
        )

        cif = os.path.join(
            out_dir,
            crystal,
            "{}.ligands.cif".format(
                os.path.basename(os.path.realpath(pdb).split(".pdb")[0])
            ),
        )

    return cif, params, free_mtz


def get_col_labels(out_dir, crystal, mtz):

    os.system(
        "cd {};mtzdmp {}>{}".format(
            os.path.join(out_dir, crystal),
            mtz,
            os.path.join(out_dir, crystal, "mtzdmp.txt"),
        )
    )
    column_num = 0
    with open(os.path.join(out_dir, crystal, "mtzdmp.txt"), "r") as mtzdmp_file:
        for line_num, line in enumerate(mtzdmp_file.readlines()):
            if "* Column Labels :" in line:
                column_num = line_num + 2
                continue
            if column_num != 0:
                if line_num == column_num:
                    col_label = line
                    break

    if "SIGF" and "F" and "IMEAN" and "SIGIMEAN" in col_label:
        column_labels = "IMEAN,SIGIMEAN"
    else:
        column_labels = None

    return column_labels


def get_most_recent_quick_refine(input_dir):
    """
    Find the most resent quick-refine log in a folder

    Parameters
    ----------
    input_dir: str
        path to directory to search

    Returns
    -------
    quick_refine_log: str
        path to quick refine log

    Raises
    ------
    FileNotFoundError
        If the quick refine log cannot be found then raise error
    ValueError
        raised if the input is not a directory


    """
    # Initialise value
    quick_refine_log = None
    # Check for existence of supplied input directory
    if not os.path.isdir(input_dir):
        ValueError("Input string is not a directory")

    # Find all subfolders
    subfolders = [f.name for f in os.scandir(input_dir) if f.is_dir()]

    # Search for highest number, and thus most recent folder
    max_num = 0
    recent_refinement = None

    for folder in subfolders:

        # TMP fodlers to be ignored
        if "TMP" in folder:
            shutil.rmtree(os.path.join(input_dir, folder))
            continue

        if "refine" not in folder:
            continue

        num = int(folder.split("_")[1])
        if num > max_num:
            max_num = num

    # Get folder path with highest number in
    for folder in subfolders:
        if str(max_num) in folder:
            recent_refinement_path = os.path.join(input_dir, folder)

            refine_folder_file_names = [
                f.name for f in os.scandir(recent_refinement_path) if f.is_file()
            ]

            for refine_folder_file in refine_folder_file_names:

                if "quick-refine.log" in refine_folder_file:
                    quick_refine_log = os.path.join(
                        recent_refinement_path, refine_folder_file
                    )

    # Return if file exists, otehrwise raise error
    if quick_refine_log is None:
        raise FileNotFoundError("{} not found".format(quick_refine_log))
    elif os.path.isfile(quick_refine_log):
        return quick_refine_log
