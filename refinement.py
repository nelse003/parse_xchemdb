import os
import glob
from shutil import copyfile

def path_from_refine_pdb(pdb, glob_string):

    path = os.path.dirname(pdb)
    files_list = glob.glob(os.path.join(path, glob_string))

    if len(files_list) >= 1:
        return files_list[0]
    elif path == "/":
        return

    return path_from_refine_pdb(path, glob_string=glob_string)

def free_mtz_path_from_refine_pdb(pdb):
    return path_from_refine_pdb(pdb, glob_string='*.free.mtz')


def cif_path(cif='', pdb=''):

    if cif is '' and pdb is '':
        raise ValueError('Path and cif cannot both be None')

    if cif is not '':
        if os.path.exists(cif):
            cif_path = cif
        elif pdb is not '':
            cif = os.path.basename(cif)
            cif_path = path_from_refine_pdb(pdb=pdb, glob_string=cif)
        else:
            raise ValueError('Cif path cannot be found, '
                             'try providing pdb path')
    else:
        cif_path = path_from_refine_pdb(pdb, glob_string="*.cif")

    if cif_path is None:
        raise  ValueError('Cif path cannot be found')

    return cif_path

def update_refinement_params(params, extra_params):
    params = open(params,'a')
    params.write('\n' + extra_params)
    params.close()

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

    #TODO move to luigi parameter
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

def write_refmac_csh(crystal,
                     pdb,
                     cif,
                     out_dir,
                     refinement_script_dir,
                     extra_params="NCYC=50",
                     free_mtz='',
                     params=''):

    """
    Write a csh script to run giant.quick_refine
    
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
    
    
    Notes
    ---------


    """

    refinement_program = "refmac"

    # If Cif file is not found at supplied location (due to error in database),
    # or it is not supplied, it's implicit location:
    # (same folder as refine.pdb symlink) is checked
    cif = cif_path(cif=cif, pdb=pdb)

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

    # os.path.isfile checks symlinks to see if path leads to a file.
    elif not os.path.isfile(free_mtz):
        free_mtz = free_mtz_path_from_refine_pdb(pdb)

    # If parameter file is not provided,
    # search the relativ_path for the pdb file
    if params is '':
        params = path_from_refine_pdb(pdb, glob_string="*{}*params".format(refinement_program))



    input_dir = os.path.join(out_dir, crystal)

    if not os.path.exists(refinement_script_dir):
        os.makedirs(refinement_script_dir)

    if not os.path.exists(input_dir):
        os.makedirs(input_dir)

    input_cif = os.path.join(input_dir, "input.cif")
    input_pdb = os.path.join(input_dir, "input.pdb")
    input_params = os.path.join(input_dir, "input.params")
    input_mtz = os.path.join(input_dir, "input.mtz")

    if not os.path.exists(input_cif):
        os.symlink(cif, input_cif)

    if not os.path.exists(input_pdb):
        os.symlink(pdb, input_pdb)

    if not os.path.exists(input_params):
        copyfile(params, input_params)

    if not os.path.exists(input_mtz):
        os.symlink(free_mtz, input_mtz)

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


if __name__ == "__main__":

    crystal = "DCLRE1AA-x1010"

    pdb = "/dls/labxchem/data/2016/lb13385-66/processing/" \
          "analysis/run3-Apr17/DCLRE1AA-x1010/Refine_9/refine_9.pdb"

    cif = "compound/FMOPL000676a.cif"

    out_dir = "/dls/science/groups/i04-1/elliot-dev/Work/" \
              "exhaustive_parse_xchem_db/convergence_refinement"

    refinement_script_dir = "/dls/science/groups/i04-1/elliot-dev/Work/exhaustive_parse_xchem_db/tmp"

    write_refmac_csh(crystal=crystal,
                     pdb=pdb,
                     cif=cif,
                     out_dir=out_dir,
                     refinement_script_dir=refinement_script_dir,
                     extra_params="NCYC=3",
                     free_mtz=None,
                     params=None)







