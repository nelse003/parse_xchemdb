import os
import glob
import subprocess
import shutil
import ast
import pandas as pd
import numpy as np

from parse_xchemdb import smiles_from_crystal

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
    if len(files_list) == 1:
        return files_list[0]

    # If multiple paramter files are present
    elif len(files_list) >= 1:
        for file in files_list:

            # If the name of the refinement program
            # appears in the file string
            if refinement_program in file:
                return file

            # Otherwise check the file contents
            elif find_program_from_parameter_file(file) == refinement_program:
                return file

    elif path == "/":
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


def update_refinement_params(params, extra_params):
    """
    Append new parameters to the parameter file

    Parameters
    ----------
    params: str
        path to parameter file
    extra_params: str
        extra parameters to be added

    Returns
    -------
    None
    """

    params = open(params,'a')
    params.write('\n' + extra_params)
    params.close()


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
    file_txt = open(file, 'r').read()
    if string in file_txt:
        return True
    else:
        return False

def split_conformations(pdb, working_dir=None):
    """Run giant.split conformations preserving occupancies

    Parameters
    ----------
    pdb: str
        path to input pdb with merged conformations

    """

    if not os.path.isdir(working_dir):
        os.makedirs(working_dir)

    if working_dir is not None:
        os.chdir(working_dir)

    cmd =(
    'giant.split_conformations' +
    " input.pdb='{}'".format(pdb) +
    ' reset_occupancies=False' +
    ' suffix_prefix=split'
    )
    os.system(cmd)


def write_refmac_csh(pdb,
                     crystal,
                     cif,
                     mtz,
                     out_dir,
                     refinement_script_dir,
                     script_dir,
                     ncyc=50,
                     refinement_type=None,
                     ccp4_path="/dls/science/groups/i04-1/" \
                               "software/pandda_0.2.12/ccp4/ccp4-7.0/bin/"\
                               "ccp4.setup-sh"):

    """
    Write .csh script to use refmac

    Parameters
    -----------
    pdb: str
        path to pdb file. Either split.bound.pdb
        or split.ground.pdb
    crystal: str
        crystal name
    cif: str
        path to cif file
    mtz: str
        path to mtz file
    ncyc: int
        number of cycles
    out_dir: str
        path to output directory
    refinement_script_dir: str
        path to directory where csh files will be written
    refinement_type: str
        type of refinement

    Returns
    -------
    None


    Notes
    -------

    """
    # Qsub specific line
    pbs_line = '#PBS -joe -N \n'

    if "bound" in pdb:
        refinement_type = "bound"
    elif "ground" in pdb:
        refinement_type = "ground"
    else:
        refinement_type = ""

    # get name of pdb after giant.split_conformations
    if refinement_type == "bound":
        pdb_dir = os.path.dirname(pdb)
        pdb = os.path.join(pdb_dir, "input.split.bound-state.pdb")
    elif refinement_type == "ground":
        pdb_dir = os.path.dirname(pdb)
        pdb = os.path.join(pdb_dir, "input.split.ground-state.pdb")

    # Parse PDB to get ligand as occupancy groups as string
    occ_group = get_occ_groups(tmp_dir=refinement_script_dir,
                               crystal=crystal,
                               pdb=pdb,
                               script_dir=script_dir,
                               ccp4_path=ccp4_path)

    out_mtz = os.path.join(out_dir, "refine.mtz")
    out_pdb = os.path.join(out_dir, "refine.pdb")
    out_cif = os.path.join(out_dir, "refine.cif")
    log = os.path.join(out_dir, "refmac.log".format(refinement_type))

    source = "source {}".format(ccp4_path)

    Cmds = (
            '#!' + os.getenv('SHELL') + '\n'
            + pbs_line +
            '\n'
            + source +
            '\n'
            +
            "refmac5 HKLIN {} \\".format(mtz) + '\n' +
            "HKLOUT {} \\".format(out_mtz) + '\n' +
            "XYZIN {} \\".format(pdb) + '\n' +
            "XYZOUT {} \\".format(out_pdb) + '\n' +
            "LIBIN {} \\".format(cif) + '\n' +
            "LIBOUT {} \\".format(out_cif) + '\n' +

             "<< EOF > {}".format(log) + '\n' +
"""
make -
    hydrogen ALL -
    hout NO -
    peptide NO -
    cispeptide YES -
    ssbridge YES -
    symmetry YES -
    sugar YES -
    connectivity NO -
    link NO
refi -
    type REST -
    resi MLKF -
    meth CGMAT -
    bref ISOT""" + "\n"

    "ncyc {}".format(ncyc) + "\n" +

    """scal -
    type SIMP -
    LSSC -
    ANISO -
    EXPE
weight matrix 0.25
solvent YES
monitor MEDIUM -
    torsion 10.0 -
    distance 10.0 -
    angle 10.0 -
    plane 10.0 -
    chiral 10.0 -
    bfactor 10.0 -
    bsphere 10.0 -
    rbond 10.0 -
    ncsr 10.0
labin  FP=F SIGFP=SIGF FREE=FreeR_flag
labout  FC=FC FWT=FWT PHIC=PHIC PHWT=PHWT DELFWT=DELFWT PHDELWT=PHDELWT FOM=FOM"""
            + "\n" +
            occ_group +
            "DNAME {}".format(crystal) + '\n' +
            "END\nEOF"

            )

    # File location and name
    csh_file = os.path.join(refinement_script_dir, "{}_{}.csh".format(crystal, refinement_type))

    # Write file
    cmd = open(csh_file, 'w')
    cmd.write(Cmds)
    cmd.close()



def write_quick_refine_csh(refine_pdb,
                           cif,
                           free_mtz,
                           crystal,
                           refinement_params,
                           out_dir,
                           refinement_script_dir,
                           refinement_program='refmac',
                           refinement_type="superposed",
                           out_prefix="refine_",
                           dir_prefix="refine_",
                           ccp4_path="/dls/science/groups/i04-1/"\
        "software/pandda_0.2.12/ccp4/ccp4-7.0/bin/ccp4.setup-sh"):

    """
    Write .csh script to refine using giant.quick_refine

    Writes script to run on cluster with giant.quick_refine
    and two calls to split, with and without resetting occupancies

    Parameters
    ----------
    ccp4_path
    refine_pdb: str
        path to pdb file
    cif:
        path to cif file
    free_mtz:
        path to mtz file
    crystal: str
        crystal name
    refinement_params: str
        path to parameter file
    out_dir: str
        path to output directory for refinement files
    refinement_script_dir: str
        path to directory where csh files will be written
    refinement_program: str
        refinemnt proggram to be used; phenix or refmac
    out_prefix: str
        prefix of the refinement file
    dir_prefix: str
        prefix of the refinemtn directory

    Returns
    -------
    None

    Notes
    -------
    """

    # Qsub specific line
    pbs_line = '#PBS -joe -N \n'

    # TODO Test whether this module load is needed,
    #       as there are now no phenix dependent lines
    module_load = ''
    if os.getcwd().startswith('/dls'):
        module_load = 'module load phenix\n'

    source = "source {}".format(ccp4_path)

    # Shell suitable string for csh file
    Cmds = (
            '#!' + os.getenv('SHELL') + '\n'
            + pbs_line +
            '\n'
            + module_load +
            '\n'
            + source +
            '\n' +
            'cd {}\n'.format(out_dir) +
            'giant.quick_refine'
            ' input.pdb=%s' % refine_pdb +
            ' mtz=%s' % free_mtz +
            ' cif=%s' % cif +
            ' program=%s' % refinement_program +
            ' params=%s' % refinement_params +
            " dir_prefix='%s'" % dir_prefix +
            " out_prefix='%s'" % out_prefix  +
            " split_conformations='False'"
            '\n'
            'cd ' + os.path.join(out_dir,dir_prefix + '0001') + '\n'
            'giant.split_conformations'
            " input.pdb='%s.pdb'" % out_prefix +
            ' reset_occupancies=False'
            ' suffix_prefix=split'
            '\n'
            'giant.split_conformations'
            " input.pdb='%s.pdb'" % out_prefix +
            ' reset_occupancies=True'
            ' suffix_prefix=output '
            '\n'
    )

    # File location and name
    csh_file = os.path.join(refinement_script_dir,
                            "{}_{}.csh".format(crystal,refinement_type))

    # Write file
    cmd = open(csh_file, 'w')
    cmd.write(Cmds)
    cmd.close()


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
    if cif is not None:
        input_cif = os.path.join(input_dir, "input.cif")
        if not os.path.exists(input_cif):
            os.symlink(cif, input_cif)
    else:
        input_cif = None

    if pdb is not None:
        input_pdb = os.path.join(input_dir, "input.pdb")
        if not os.path.exists(input_pdb):
            os.symlink(pdb, input_pdb)
    else:
        input_pdb = None

    if params is not None:
        input_params = os.path.join(input_dir, "input.params")
        if not os.path.exists(input_params):
            shutil.copyfile(params, input_params)
    else:
        input_params = None

    if free_mtz is not None:
        input_mtz = os.path.join(input_dir, "input.mtz")
        if not os.path.exists(input_mtz):
            os.symlink(free_mtz, input_mtz)
    else:
        input_mtz = None

    return input_cif, input_pdb, input_params, input_mtz


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

    # Check that the source files for the symlinks exist
    if not os.path.isfile(cif):
        raise FileNotFoundError("{}: cif Not found".format(cif))
    if not os.path.isfile(params):
        raise FileNotFoundError("{}: parameter file Not found".format(params))
    if not os.path.isfile(free_mtz):
        raise FileNotFoundError("{}: mtz Not found".format(free_mtz))

    return cif, params, free_mtz


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
    if '.cif' in out_file:
        out_file = out_file.split('.cif')[0]

    # Wrapping a basic command line implementation of acedrg
    a = subprocess.run(['acedrg','-i',smiles,'-o',out_file],
                       stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE,
                       encoding='utf-8')

    # Error specific to cif file not being produced
    smiles_error = "Can not generate a molecule from the input SMILES string!"

    # Check the stdout of acedrg for the error
    if smiles_error in a.stdout:
        raise FileNotFoundError("{}.cif cannot be produced from the smiles using acedrg")

    # Check whether there is a output file at all
    if not os.path.isfile(out_file):
        raise FileNotFoundError("{}.cif was not produced from the smiles using acedrg")

    # specify temporary folder name
    tmp_folder = input_cif + "_TMP"

    # remove intermediary _TMP folder
    if os.path.isdir(tmp_folder):
        shutil.rmtree(tmp_folder)


def lig_pos_to_occupancy_refinement_string(lig_pos):
    """
    Write occupancy refinement parameters for refmac single model

    Parameters
    ----------
    lig_pos

    Returns
    -------

    """
    group_lines = []
    incomplete_lines = []

    # Loop over ligand information
    for occ_group, pos in enumerate(lig_pos):

        # Get chain and residue number
        chain = pos[0]
        resid = pos[2]

        # Format lines
        # Occupancy group is seperate for each residue,
        # and comes from the iterable
        group_line = "occupancy group id {} chain {} resi {}".format(occ_group+1, chain, resid)
        incomplete_line = "occupancy group alts incomplete {}".format(occ_group+1)

        # Append lines to lists
        group_lines.append(group_line)
        incomplete_lines.append(incomplete_line)

    # Write saved lines as single string
    refinement_str = '\n'.join(group_lines) +'\n' + \
                     '\n'.join(incomplete_lines) + '\n' + \
                     'occupancy refine' + '\n'

    return refinement_str


def get_occ_groups(tmp_dir,
                   crystal,
                   pdb,
                   script_dir,
                   ccp4_path):
    """
    Get occupancy groups from pdb by calling ligand.py

    Parameters
    ----------
    tmp_dir: str
        path to directory where tmp files will be written
    crystal: str
        crystal name
    pdb: str
        path to pdb file to be parsed
    ccp4_path: str
        path to source ccp4 distribution from

    Returns
    -------
    occ_group: str
        A string to be appended to the refinment csh,
        containing occupancy groups incomplete for each residue
        which is thought to be a ligand of interest

    """

    # Read pdb and determine ligand occupancy groups
    tmp_file = os.path.join(tmp_dir, "lig_pos_tmp_{}.txt".format(crystal))
    source = "source {}".format(ccp4_path)
    os.system(source)
    ligand_py = os.path.join(script_dir, "ccp4/ligand.py")
    os.system("ccp4-python {} {} {}".format(ligand_py, pdb, tmp_file))
    with open(tmp_file) as f:
        lig_pos = f.read()
    os.remove(tmp_file)

    # Convert string in file to list
    lig_pos = ast.literal_eval(lig_pos)
    occ_group = lig_pos_to_occupancy_refinement_string(list(lig_pos))

    return occ_group


def prepare_refinement(pdb,
                       crystal,
                       cif,
                       mtz,
                       ncyc,
                       out_dir,
                       refinement_script_dir,
                       refinement_type,
                       script_dir,
                       ccp4_path="/dls/science/groups/i04-1/" \
                               "software/pandda_0.2.12/ccp4/ccp4-7.0/bin/" \
                               "ccp4.setup-sh"):

    """
    Prepare refinement csh for refmac without superposed state

    Parameters
    -----------
    pdb: str
        path to pdb file. Either split.bound.pdb
        or split.ground.pdb
    crystal: str
        crystal name
    cif: str
        path to cif file
    mtz: str
        path to mtz file
    ncyc: int
        number of cycles
    out_dir: str
        path to output directory
    refinement_script_dir: str
        path to directory where csh files will be written
    ccp4_path: str
        path to ccp4 to be used for ccp4 python/ giant scripts


    Notes
    -----
    """

    # Generate working directories
    input_dir = os.path.join(out_dir, crystal)
    if not os.path.exists(refinement_script_dir):
        os.makedirs(refinement_script_dir)

    if not os.path.exists(input_dir):
        os.makedirs(input_dir)

    # TODO Replace check inputs without params check

    # Check and replace inputs with existing files,
    # or regenerate if necessary
    cif, params, mtz = check_inputs(cif=cif,
                                    pdb=pdb,
                                    params='',
                                    free_mtz=mtz,
                                    refinement_program="refmac",
                                    input_dir=input_dir,
                                    crystal=crystal)


    # generate symlinks to refinement files
    input_cif, \
    input_pdb, \
    input_params, \
    input_mtz = make_symlinks(input_dir=input_dir,
                              cif=cif,
                              pdb=pdb,
                              params=None,
                              free_mtz=mtz)

    write_refmac_csh(pdb=input_pdb,
                     crystal=crystal,
                     cif=input_cif,
                     mtz=input_mtz,
                     out_dir=input_dir,
                     refinement_script_dir=refinement_script_dir,
                     ncyc=ncyc,
                     script_dir=script_dir,
                     refinement_type=refinement_type,
                     ccp4_path=ccp4_path)


def prepare_superposed_refinement(crystal,
                              pdb,
                              cif,
                              out_dir,
                              refinement_script_dir,
                              refinement_type="superposed",
                              extra_params="NCYC=50",
                              free_mtz='',
                              params=''):

    """
    Prepare files and write csh script to run giant.quick_refine

    Creates directories to hold refinement scripts, and results.
    Checks and replaces if necessary input files (cif, pdnb, params, mtz),
    creating symlinks in refinement folder.
    Check whether refinement has failed due to restraints mismatch,
    regenerate restraints if needed.
    Add extra parameters to parameter file.
    Write refinement csh script file.


    Parameters
    -----------
    crystal: str
        crystal name
    cif: str
        path to cif file
    out_dir: str
        path to output directory
    refinement_script_dir: str
        path to refinement script directory
    extra_params: str
        extra parameters to be appended to parameter
        file before refinement
    free_mtz: str
        path to free mtz
    params: str
        path to parameter file

    Returns
    --------
    None

    (Converegence refinement) Failure modes:

    Issues that have been fixed

    1) Only cif and PDB found.
       No csh file is created

        Examples:

        HPrP-x0256
        STAG-x0167

        Looking at

        STAG1A-x0167

        The search path:

        /dls/labxchem/data/2017/lb18145-52/processing/analysis/initial_model/STAG1A-x0167/Refine_0002

        is not the most recent refinement.
        In that search path there is a input.params file.

        Solution:

        Search for a parameter file,
        changed to look for any file matching parameter file in folder.
        If multiple are present,
        check that the refinement program matches

        Secondary required solution:

        Also search for .mtz file,
        if search for .free.mtz fails.
        Edit to write_refmac_csh()

        No folders have no quick-refine.log

    2) cif missing

        Recursively search
        If not found get smiles from DB
        run acedrg
        If acedrg fails raise FileNotFoundError

        Examples:

        UP1-x0030: Has an input cif file, but won't refine due mismatch in cif file:

            atom: "C01 " is absent in coord_file
            atom: "N02 " is absent in coord_file
            atom: "C03 " is absent in coord_file
            atom: "C04 " is absent in coord_file
            atom: "N05 " is absent in coord_file
            atom: "C06 " is absent in coord_file
            atom: "C07 " is absent in coord_file
            atom: "C08 " is absent in coord_file
            atom: "O09 " is absent in coord_file
            atom: "O11 " is absent in coord_file
            atom: "C15 " is absent in coord_file
            atom: "C16 " is absent in coord_file
            atom: "C17 " is absent in coord_file
            atom: "C18 " is absent in coord_file
            atom: "C1  " is absent in lib description.
            atom: "N1  " is absent in lib description.
            atom: "C2  " is absent in lib description.
            atom: "C3  " is absent in lib description.
            atom: "N2  " is absent in lib description.
            atom: "C4  " is absent in lib description.
            atom: "C5  " is absent in lib description.
            atom: "C6  " is absent in lib description.
            atom: "O1  " is absent in lib description.
            atom: "C7  " is absent in lib description.
            atom: "O2  " is absent in lib description.
            atom: "C8  " is absent in lib description.
            atom: "C9  " is absent in lib description.
            atom: "C11 " is absent in lib description.

    3) Refinement fails due to external distance restraints not being satisfiable.

        Examples:

        FIH-x0241
        FIH-x0379
        VIM2-MB-403
        NUDT7A_Crude-x0030

        Solution

        If identified as issue rerun giant.make_restraints

    """

    # Generate working directories
    input_dir = os.path.join(out_dir, crystal)
    if not os.path.exists(refinement_script_dir):
        os.makedirs(refinement_script_dir)

    if not os.path.exists(input_dir):
        os.makedirs(input_dir)

    # Check and replace inputs with existing files,
    # or regenerate if necessary
    cif, params, free_mtz = check_inputs(cif=cif,
                                         pdb=pdb,
                                         params=params,
                                         free_mtz=free_mtz,
                                         refinement_program="refmac",
                                         input_dir=input_dir,
                                         crystal=crystal)


    # generate symlinks to refinement files
    input_cif, \
    input_pdb, \
    input_params, \
    input_mtz = make_symlinks(input_dir=input_dir,
                              cif=cif,
                              pdb=pdb,
                              params=params,
                              free_mtz=free_mtz)

    # Check for failed refinement due to restraint error
    # Run giant.make_restraints in this case
    if check_restraints(input_dir):

        # TODO Move source to a parameter
        os.system("source /dls/science/groups/i04-1/software/"
                  "pandda_0.2.12/ccp4/ccp4-7.0/bin/ccp4.setup-sh")

        os.chdir(input_dir)
        os.system("giant.make_restraints {}".format(input_pdb))

        link_pdb = os.path.join(input_dir,"input.link.pdb")
        new_refmac_restraints = os.path.join(input_dir,
                        'multi-state-restraints.refmac.params')

        if os.path.isfile(link_pdb):
            input_pdb = link_pdb

        if os.path.isfile(new_refmac_restraints):
            input_params = new_refmac_restraints

    # Check that refinement is failing due to a cif file missing
    if check_refinement_for_cif_error(input_dir):
        smiles = smiles_from_crystal(crystal)
        cif_backup = os.path.join(input_dir, "backup.cif")
        os.rename(input_cif, cif_backup)
        smiles_to_cif_acedrg(smiles, input_cif)


    update_refinement_params(params=input_params, extra_params=extra_params)

    write_quick_refine_csh(refine_pdb=input_pdb,
                           cif=input_cif,
                           free_mtz=input_mtz,
                           crystal=crystal,
                           refinement_params=input_params,
                           out_dir=input_dir,
                           refinement_script_dir=refinement_script_dir,
                           refinement_program='refmac',
                           refinement_type=refinement_type,
                           out_prefix="refine_1",
                           dir_prefix="refine_")


def state_occ(row, bound, ground, pdb):
    if row.pdb_latest == pdb:
        if row.state == "bound":
            return bound
        if row.state == "ground":
            return ground


def state_occupancies(occ_state_comment_csv, occ_correct_csv):
    """
    Sum occupancies in occupancies for full states

    Adds convergence ratio
    x(n)/(x(n-1) -1)
    to csv.

    Adds up occupancy for ground and bound states respectively
    across each complete group

    Parameters
    ----------
    occ_state_comment_csv: str
        path to csv with occupancy convergence information
        for each residue involved in complete groups
    occ_correct_csv: str
        path to csv with convergence information,
        and occupancy for the "bound" or "ground" state

    Returns
    -------
    None
    """

    # Read CSV
    occ_df = pd.read_csv(occ_state_comment_csv, index_col=[0, 1])

    # Select only residues that are correctly occupied
    occ_correct_df = occ_df[occ_df['comment'] == 'Correctly Occupied']

    # print(occ_correct_df.head())
    # print(occ_correct_df.columns.values)

    # TODO Fix to run where rows are different lengths

    int_cols = []
    for col in occ_correct_df.columns.values:
        try:
            int_cols.append(int(col))
        except ValueError:
            continue
    str_cols = list(map(str, int_cols))
    df = occ_correct_df[str_cols]

    # TODO Find a more robust convergence metric

    occ_correct_df['converge'] = abs(df[str_cols[-1]] / df[str_cols[-2]] - 1)

    # Select the final occupancy value
    occ_correct_df['occupancy'] = df[str_cols[-1]]

    pdb_df_list = []
    for pdb in occ_correct_df.pdb_latest.unique():

        bound = 0
        ground = 0

        pdb_df = occ_correct_df.loc[
            (occ_correct_df['pdb_latest'] == pdb)]

        grouped = pdb_df.groupby(['complete group', 'occupancy', 'alte', 'state'])

        for name, group in grouped:

            group_occ = group.occupancy.unique()[0]

            if "ground" in group.state.unique()[0]:
                ground += group_occ

            if "bound" in group.state.unique()[0]:
                bound += group_occ

        print(ground + bound)
        try:
            np.testing.assert_allclose(ground + bound, 1.0, atol=0.01)
        except AssertionError:
            continue

        pdb_df['state occupancy'] = pdb_df.apply(
            func=state_occ,
            bound=bound,
            ground=ground,
            pdb=pdb,
            axis=1)

        pdb_df_list.append(pdb_df)

    occ_correct_df = pd.concat(pdb_df_list)
    occ_correct_df.to_csv(occ_correct_csv)

# TODO Split state_occupancies into below function and second function, split associated task

def convergence_state_by_refinement_type(occ_csv, occ_conv_state_csv, refinement_type):

    """
    Add convergence and occupancy and state to csv which has at least convergence columns

    Parameters
    ----------
    occ_csv: str
        Path to input csv which contains occupancy, but not necessarily
    occ_conv_csv: str
        Path to output csv
    refinement_type: str
        "bound" or "ground"

    Returns
    -------

    """
    occ_df = pd.read_csv(occ_csv)

    int_cols = []
    for col in occ_df.columns.values:
        try:
            int_cols.append(int(col))
        except ValueError:
            continue
    str_cols = list(map(str, int_cols))
    df = occ_df[str_cols]

    occ_df['state'] = refinement_type
    occ_df['converge'] = abs(df[str_cols[-1]] / df[str_cols[-2]] - 1)
    occ_df['state occupancy'] = df[str_cols[-1]]

    occ_df.to_csv(occ_conv_state_csv)