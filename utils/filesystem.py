import glob
import os
import shutil

from utils.smiles import smiles_from_crystal
from utils.smiles import smiles_to_cif_acedrg


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

    file_txt = open(file, 'r').read()

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

    return parameter_from_refine_pdb(path,
                                     glob_string=glob_string,
                                     refinement_program=refinement_program)


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
    return path_from_refine_pdb(pdb, glob_string='*.free.mtz')


def cif_path(cif='', pdb='', input_dir=None, crystal=None):
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

    if cif is not '':
        # Check file existence
        if os.path.exists(cif):
            cif_p = cif
        elif pdb is not '':
            cif = os.path.basename(cif)
            # Check for named cif file implicit in the pdb location
            cif_p = path_from_refine_pdb(pdb=pdb, glob_string=cif)
        else:
            raise FileNotFoundError('Cif path cannot be found, '
                                    'try providing pdb path')
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
    if cif_p is '' and pdb is '':
        raise FileNotFoundError('Path and cif cannot both be None')


    return cif_p


def check_inputs(cif,
                 pdb,
                 params,
                 free_mtz,
                 refinement_program,
                 input_dir,
                 crystal):
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

    # If Cif file is not found at supplied location (due to error in database),
    # or it is not supplied, it's implicit location:
    # (same folder as refine.pdb symlink) is checked.
    # If that doesn't work try generating from smiles string
    cif = cif_path(cif=cif,
                   pdb=pdb,
                   input_dir=input_dir,
                   crystal=crystal)

    # If pdb file does not exist raise error
    if not os.path.isfile(pdb):
        raise FileNotFoundError("{}: pdb Not found".format(pdb))

    # Check that free_mtz is provided.
    # An empty string is used rather than None,
    # due to luigi.Parameters passing strings around.
    # If it doesn't exist try to infer location relative
    # to the provided pdb.
    if free_mtz == '':
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
    if params is '':
        params = parameter_from_refine_pdb(pdb,
                                           glob_string="*params",
                                           refinement_program=refinement_program)
    #If the refinement program does not amtch the parameter file,
    # search for one that does in relative_path for the pdb file
    if find_program_from_parameter_file(params) != refinement_program:
        params = parameter_from_refine_pdb(pdb,
                                           glob_string="*params",
                                           refinement_program=refinement_program)

    # Check that the source files for the symlinks exist
    if not os.path.isfile(cif):
        raise FileNotFoundError("{}: cif Not found".format(cif))
    if not os.path.isfile(params):
        raise FileNotFoundError("{}: parameter file Not found".format(params))
    if not os.path.isfile(free_mtz):
        raise FileNotFoundError("{}: mtz Not found".format(free_mtz))

    return cif, params, free_mtz


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
        if 'TMP' in folder:
            shutil.rmtree(os.path.join(input_dir, folder))
            continue

        num = int(folder.split('_')[1])
        if num > max_num:
            max_num = num

    # Get folder path with highest number in
    for folder in subfolders:
        if str(max_num) in folder:
            recent_refinement = folder
            recent_refinement_path = os.path.join(input_dir, recent_refinement)
            quick_refine_log = os.path.join(recent_refinement_path, "refine_1.quick-refine.log")

    # Return if file exists, otehrwise raise error
    if quick_refine_log is None:
        raise FileNotFoundError("{} not found".format(quick_refine_log))
    elif os.path.isfile(quick_refine_log):
        return quick_refine_log