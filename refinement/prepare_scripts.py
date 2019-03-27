import os

from utils.smiles import smiles_from_crystal
from refinement.call_ccp4 import get_occ_groups
from utils.filesystem import check_inputs
from refinement.check_refienement_failure import check_restraints
from refinement.check_refienement_failure import check_refinement_for_cif_error
from utils.smiles import smiles_to_cif_acedrg
from utils.symlink import make_symlinks


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
    log = os.path.join(out_dir, "refmac.log")

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