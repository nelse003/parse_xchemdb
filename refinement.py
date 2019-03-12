import os
import glob
from shutil import copyfile
from parse_xchemdb import smiles_from_crystal

def find_program_from_parameter_file(file):
    """ Read parameter file to determine whether refmac or phenix

    May not be sufficient for all files,
    as checks may miss some parts of file
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

    print("CIF" + cif)

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

        # TODO change to subprocess:
        # http://www.dalkescientific.com/writings/diary/archive/2005/04/12/wrapping_command_line_programs.html

        os.system("acedrg -i '{}' -o {}".format(smiles, input_cif))
        cif_p = input_cif

    return cif_p

def update_refinement_params(params, extra_params):
    params = open(params,'a')
    params.write('\n' + extra_params)
    params.close()


def check_restraints(input_dir):
    """ Check whether the quick refine is failing due restraint mismatch """
    quick_refine_log = get_most_recent_quick_refine(input_dir)
    return check_file_for_string(quick_refine_log,
                                 "Error: At least one of the atoms from"
                                 " the restraints could not be found")

def check_cif_refinement(input_dir):
    """ Check whether the refinement has failed due to a cif error """
    quick_refine_log = get_most_recent_quick_refine(input_dir)
    return check_file_for_string(quick_refine_log,
                                 "Refmac:  New ligand has been encountered. Stopping now")


def get_most_recent_quick_refine(input_dir):

    subfolders = [f.name for f in os.scandir(input_dir) if f.is_dir()]
    max_num = 0
    recent_refinement = None
    for folder in subfolders:
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
    """Check if string is in file"""
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
                           qsub_name="ERN refine",
                           refinement_program='refmac',
                           out_prefix="refine_",
                           dir_prefix="refine_"):

    pbs_line = '#PBS -joe -N {}\n'.format(qsub_name)

    module_load = ''
    if os.getcwd().startswith('/dls'):
        module_load = 'module load phenix\n'

    # TODO move to luigi parameter
    source = "source /dls/science/groups/i04-1/software/pandda_0.2.12/ccp4/ccp4-7.0/bin/ccp4.setup-sh"

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
    csh_file = os.path.join(refinement_script_dir, "{}.csh".format(crystal))
    cmd = open(csh_file, 'w')
    cmd.write(refmacCmds)
    cmd.close()

def make_symlinks(input_dir, cif, pdb, params, free_mtz):

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
        copyfile(params, input_params)

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

    # If Cif file is not found at supplied location (due to error in database),
    # or it is not supplied, it's implicit location:
    # (same folder as refine.pdb symlink) is checked.
    # If that doesn't work try generating from smiles string
    cif = cif_path(cif=cif,
                   pdb=pdb,
                   input_dir=input_dir,
                   crystal=crystal)

    # Check the pdb is file, raise exception otherwise,
    # as this is required for implicit location of other files
    if not os.path.isfile(pdb):
        raise ValueError("{}: is not a valid file path for a pdb file".format(pdb))

    # Check that free_mtz is provided.
    # An empty string is used rather than None,
    # due to luigi.Parameters passing strings around.
    # If it doesn't exist try to infer location relative
    # to the provided pdb.

    if free_mtz == '':
        free_mtz = free_mtz_path_from_refine_pdb(pdb)

    print(free_mtz)

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
    if not os.path.isfile(pdb):
        raise FileNotFoundError("{}: pdb Not found".format(pdb))
    if not os.path.isfile(params):
        raise FileNotFoundError("{}: parameter file Not found".format(params))
    if not os.path.isfile(free_mtz):
        raise FileNotFoundError("{}: mtz Not found".format(free_mtz))

    return cif, pdb, params, free_mtz

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
    cif, pdb, params, free_mtz = check_inputs(cif=cif,
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
    # Run giant.make_restraitns in this case
    # TODO Move source to a parameter
    if check_restraints(input_dir):
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

    if check_cif_refinement(input_dir):
        smiles = smiles_from_crystal(crystal)
        cif_backup = os.path.join(input_dir, "backup.cif")
        os.rename(input_cif, cif_backup)

        # TODO change to subprocess & put in function:
        # http://www.dalkescientific.com/writings/diary/archive/2005/04/12/wrapping_command_line_programs.html
        os.system("acedrg -i '{}' -o {}".format(smiles, input_cif))

    update_refinement_params(params=input_params, extra_params=extra_params)

    write_quick_refine_csh(refine_pdb=input_pdb,
                           cif=input_cif,
                           free_mtz=input_mtz,
                           crystal=crystal,
                           refinement_params=input_params,
                           out_dir=input_dir,
                           refinement_script_dir=refinement_script_dir,
                           qsub_name="ERN refine",
                           refinement_program='refmac',
                           out_prefix="refine_1",
                           dir_prefix="refine_")








