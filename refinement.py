import os
import glob
import subprocess
import shutil

from parse_xchemdb import smiles_from_crystal

def find_program_from_parameter_file(file):
    """ Read parameter file to determine whether refmac or phenix

    May not be sufficient for all files,
    as checks may miss some parts of file

    Returns
    ----------
    str or None
    """

    file_txt = open(file, 'r').read()

    if "occupancy group id" in file_txt:
        return "refmac"
    elif "refinement.geometry_restraints.edits" in file_txt:
        return "phenix"
    else:
        return None

def parameter_from_refine_pdb(pdb, glob_string, refinement_program):

    """ Search recursively upwards for a parameter file"""

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
    """ Recursively search pdb path for files matching the glob string"""

    path = os.path.dirname(pdb)
    files_list = glob.glob(os.path.join(path, glob_string))

    if len(files_list) >= 1:
        return files_list[0]
    elif path == "/":
        return

    return path_from_refine_pdb(path, glob_string=glob_string)


def free_mtz_path_from_refine_pdb(pdb):
    return path_from_refine_pdb(pdb, glob_string='*.free.mtz')


def cif_path(cif='', pdb='', input_dir=None, crystal=None):

    cif_p = None
    if cif is '' and pdb is '':
        raise ValueError('Path and cif cannot both be None')

    if cif is not '':
        if os.path.exists(cif):
            cif_p = cif
        elif pdb is not '':
            cif = os.path.basename(cif)
            cif_p = path_from_refine_pdb(pdb=pdb, glob_string=cif)
        else:
            raise ValueError('Cif path cannot be found, '
                             'try providing pdb path')
    else:
        cif_p = path_from_refine_pdb(pdb, glob_string="*.cif")

    # Try finding cif using smiles
    if cif_p is None:
        input_cif = os.path.join(input_dir, "input.cif")
        smiles = smiles_from_crystal(crystal)
        smiles_to_cif_acedrg(smiles=smiles, out_file=input_cif)
        cif_p = input_cif

    return cif_p

def update_refinement_params(params, extra_params):
    params = open(params,'a')
    params.write('\n' + extra_params)
    params.close()


def check_restraints(input_dir):
    """ Check whether the quick refine is failing due restraint mismatch """
    try:
        quick_refine_log = get_most_recent_quick_refine(input_dir)
    except FileNotFoundError:
        return False
    return check_file_for_string(quick_refine_log,
                                 "Error: At least one of the atoms from"
                                 " the restraints could not be found")

def check_refinement_for_cif_error(input_dir):
    """ Check whether the refinement has failed due to a cif error """

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

    subfolders = [f.name for f in os.scandir(input_dir) if f.is_dir()]
    max_num = 0
    recent_refinement = None
    for folder in subfolders:

        if 'TMP' in folder:
            shutil.rmtree(os.path.join(input_dir, folder))
            continue

        num = int(folder.split('_')[1])
        if num > max_num:
            max_num = num
    for folder in subfolders:
        if str(max_num) in folder:
           recent_refinement = folder
    recent_refinement_path = os.path.join(input_dir, recent_refinement)
    quick_refine_log = os.path.join(recent_refinement_path, "refine_1.quick-refine.log")
    if os.path.isfile(quick_refine_log):
        return quick_refine_log
    else:
        raise FileNotFoundError("{} not found".format(quick_refine_log))


def check_file_for_string(file, string):
    """Check if string is in file

    Parameters
    ------------
    file: str
        path to file
    string: str
        string to be checked for existence in file

    Notes
    -----------
    Reads entire file into memory, so would work for very large files
    """
    file_txt = open(file, 'r').read()
    if string in file_txt:
        return True
    else:
        return False

def write_quick_refine_csh(refine_pdb,
                           cif,
                           free_mtz,
                           crystal,
                           refinement_params,
                           out_dir,
                           refinement_script_dir,
                           refinement_program='refmac',
                           out_prefix="refine_",
                           dir_prefix="refine_"):

    """
    Write .csh script to refine using giant.quick_refine

    Writes script to run on cluster with giant.quick_refine
    and two calls to split, with and without resetting occupancies

    Parameters
    ----------
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

    # TODO move to luigi parameter
    source = "source /dls/science/groups/i04-1/software/pandda_0.2.12/ccp4/ccp4-7.0/bin/ccp4.setup-sh"

    # Shell suitable string for csh file
    refmacCmds = (
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
    csh_file = os.path.join(refinement_script_dir, "{}.csh".format(crystal))

    # Write file
    cmd = open(csh_file, 'w')
    cmd.write(refmacCmds)
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
    input_cif = os.path.join(input_dir, "input.cif")
    input_pdb = os.path.join(input_dir, "input.pdb")
    input_params = os.path.join(input_dir, "input.params")
    input_mtz = os.path.join(input_dir, "input.mtz")

    # Generate symlinks if they do not exist
    if not os.path.exists(input_cif):
        os.symlink(cif, input_cif)

    if not os.path.exists(input_pdb):
        os.symlink(pdb, input_pdb)

    if not os.path.exists(input_params):
        shutil.copyfile(params, input_params)

    if not os.path.exists(input_mtz):
        os.symlink(free_mtz, input_mtz)

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


def prepare_refinement(crystal,
                       pdb,
                       cif,
                       out_dir,
                       refinement_script_dir,
                       extra_params="NCYC=50",
                       free_mtz='',
                       params=''):

    """
    Prepare files and write csh script to run giant.quick_refine

    Creates directories to hold refinement scripts, and results.
    Checks and replaces if necessary input files (cif, pdnb, params, mtz),
    creating symlinks in refinement folder.
    Check ex


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

    TODO Move refinement program to parameter

    TODO Separate into multiple short functions checking
         and updating values for each file type
    ---------
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

    # Check that cif
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
                           out_prefix="refine_1",
                           dir_prefix="refine_")

if __name__ == "__main__":
    pass




