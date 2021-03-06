import os
import sys
import importlib

#TODO Remove sys path append and repalce with proper handing of relative imports

sys.path.append('/dls/science/groups/i04-1/elliot-dev/parse_xchemdb/utils')

from smiles import smiles_from_crystal
from refinement.call_ccp4 import get_incomplete_occ_groups
from filesystem import check_inputs
from filesystem import get_col_labels
from refinement.check_refienement_failure import check_restraints
from refinement.check_refienement_failure import check_refinement_for_cif_error
from refinement.parameters import lig_pos_to_occupancy_refinement_string
from smiles import smiles_to_cif_acedrg
from symlink import make_copies_and_symlinks

from path_config import Path


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
    if extra_params is None:
        return

    params = open(params, "a")
    params.write("\n" + extra_params)
    params.close()


def write_refmac_csh(
    pdb,
    crystal,
    cif,
    mtz,
    out_dir,
    refinement_script_dir,
    script_dir,
    ncyc=50,
    ccp4_path=Path().ccp4,
    refinement_type=None,
    refinement_program="refmac",
):

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
    script_dir: str
        path to this script
    ccp4_path: str
        path to ccp4 script

    Returns
    -------
    None


    Notes
    -------

    """
    # get name of pdb after giant.split_conformations
    pdb_dir = os.path.dirname(pdb)
    if refinement_type is None:
        if "bound" in pdb:
            refinement_type = "bound"
        elif "ground" in pdb:
            refinement_type = "ground"
        else:
            refinement_type = ""

    if not os.path.isdir(refinement_script_dir):
        os.makedirs(refinement_script_dir)

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    print(refinement_script_dir)
    print(crystal)
    print(pdb)
    print(script_dir)
    print(ccp4_path)
    # Parse PDB to get ligand as occupancy groups
    lig_pos = get_incomplete_occ_groups(
        tmp_dir=refinement_script_dir,
        crystal=crystal,
        pdb=pdb,
        script_dir=script_dir,
        ccp4_path=ccp4_path,
    )
    # Turn into string
    occ_group = lig_pos_to_occupancy_refinement_string(list(lig_pos))

    # define output paths
    out_mtz = os.path.join(out_dir, "refine.mtz")
    out_pdb = os.path.join(out_dir, "refine.pdb")
    out_cif = os.path.join(out_dir, "refine.cif")
    log = os.path.join(out_dir, "refmac.log")

    with open(os.path.join(script_dir, "refinement", "refmac_template.csh")) as f:
        cmd = f.read()

    cmd = cmd.format(
        ccp4_path=ccp4_path,
        mtz=mtz,
        out_mtz=out_mtz,
        pdb=pdb,
        out_pdb=out_pdb,
        cif=cif,
        out_cif=out_cif,
        log=log,
        ncyc=ncyc,
        occ_group=occ_group,
        crystal=crystal,
    )

    # File location and name
    csh_file = os.path.join(
        refinement_script_dir,
        "{}_{}_{}.csh".format(crystal, refinement_program, refinement_type),
    )

    # Write file
    with open(csh_file, "w") as csh_f:
        csh_f.write(cmd)


def write_exhaustive_csh(
    pdb,
    mtz,
    script_dir,
    refinement_script_dir,
    out_dir,
    crystal,
    exhaustive_multiple_sampling,
    ccp4_path,
    refinement_type="superposed",
):
    """
    Write .csh script to run exhaustive search
    
    Parameters
    ----------
    pdb: pdb
        path to pdb file
    mtz: str
        path to mtz file
    script_dir:
        path to directory with input scripts (parse_xchemdb)
    refinement_script_dir: str
        path to directory for csh script
    out_dir: str
        path for output files
    crystal: str
        name of crystal
    exhaustive_multiple_sampling: str
        path to exhaustive search multiple sampling. py file
    ccp4_path: str
        path to ccp4 setup script to be sourced
    refinement_type: str
        type of refinement
    Returns
    -------
    None
    """

    with open(os.path.join(script_dir, "refinement", "exhaustive_template.csh")) as f:
        cmd = f.read()

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    cmd = cmd.format(
        ccp4_path=ccp4_path,
        out_dir=out_dir,
        exhaustive_multiple_sampling=exhaustive_multiple_sampling,
        pdb=pdb,
        mtz=mtz,
    )

    if not os.path.isdir(refinement_script_dir):
        os.makedirs(refinement_script_dir)

    # File location and name
    csh_file = os.path.join(
        refinement_script_dir,
        "{}_{}_{}.csh".format(crystal, "exhaustive", refinement_type),
    )

    with open(csh_file, "w") as csh_f:
        csh_f.write(cmd)


def write_phenix_csh(
    pdb,
    mtz,
    cif,
    script_dir,
    refinement_script_dir,
    out_dir,
    crystal,
    ncyc,
    column_labels=None,
    refinement_type="bound",
    script_filename="phenix_template.csh",
):
    """
    Write .csh script to run phenix

    Parameters
    ----------
    pdb: pdb
        path to pdb file
    mtz: str
        path to mtz file
    script_dir:
        path to directory with input scripts (parse_xchemdb)
    refinement_script_dir: str
        path to directory for csh script
    out_dir: str
        path for output files
    crystal: str
        name of crystal
    ncyc: int
        number of macrocycles to apply to phenix
    refinement_type: str
        refinement type; bound, ground or superposed
    Returns
    -------
    None
    """

    crystal_dir = os.path.join(out_dir, crystal)

    with open(os.path.join(script_dir, "refinement", script_filename)) as f:
        cmd = f.read()

    if column_labels != None:
        column_label_text = "xray_data.labels={}".format(column_labels)
    else:
        column_label_text = " "

    cmd = cmd.format(
        out_dir=out_dir,
        pdb=pdb,
        mtz=mtz,
        cif=cif,
        ncyc=ncyc,
        column_label_text=column_label_text,
    )

    if not os.path.isdir(refinement_script_dir):
        os.makedirs(refinement_script_dir)

    if not os.path.isdir(out_dir):
        os.makedirs(out_dir)

    # File location and name
    csh_file = os.path.join(
        refinement_script_dir, "{}_{}_{}.csh".format(crystal, "phenix", refinement_type)
    )

    with open(csh_file, "w") as csh_f:
        csh_f.write(cmd)

def write_phenix_b_fix_csh(
    pdb,
    mtz,
    cif,
    script_dir,
    refinement_script_dir,
    out_dir,
    crystal,
    ncyc,
    params,
):
    """
    Write .csh script to run phenix with fixed B. setup for nudt7 only

    Parameters
    ----------
    pdb: pdb
        path to pdb file
    mtz: str
        path to mtz file
    script_dir:
        path to directory with input scripts (parse_xchemdb)
    refinement_script_dir: str
        path to directory for csh script
    out_dir: str
        path for output files
    crystal: str
        name of crystal
    ncyc: int
        number of macrocycles to apply to phenix
    refinement_type: str
        refinement type; bound, ground or superposed
    Returns
    -------
    None
    """

    crystal_dir = os.path.join(out_dir, crystal)

    with open(os.path.join(script_dir, "refinement", "phenix_b_fix.csh")) as f:
        cmd = f.read()

    cmd = cmd.format(
        out_dir=out_dir,
        pdb=pdb,
        mtz=mtz,
        cif=cif,
        params=params,
    )

    if not os.path.isdir(refinement_script_dir):
        os.makedirs(refinement_script_dir)

    if not os.path.isdir(out_dir):
        os.makedirs(out_dir)

    # File location and name
    csh_file = os.path.join(
        refinement_script_dir, "{}_{}.csh".format(crystal, "phenix_b_fix")
    )

    with open(csh_file, "w") as csh_f:
        csh_f.write(cmd)


def write_buster_csh(
    pdb,
    mtz,
    cif,
    script_dir,
    refinement_script_dir,
    out_dir,
    crystal,
    refinement_type="superposed",
    refinement_program="buster",
    template_name="buster_template.csh",
):
    """
    Write .csh script to run buster

    Parameters
    ----------
    pdb: pdb
        path to pdb file
    mtz: str
        path to mtz file
    script_dir:
        path to directory with input scripts (parse_xchemdb)
    refinement_script_dir: str
        path to directory for csh script
    out_dir: str
        path for output files
    crystal: str
        name of crystal

    Returns
    -------
    None
    """

    crystal_dir = os.path.join(out_dir, crystal)

    with open(os.path.join(script_dir, "refinement", template_name)) as f:
        cmd = f.read()

    # Runs pdb to occ to generate suitable restraints into occ.gelly,
    # then used in buster refine
    cmd = cmd.format(
        out_dir=out_dir,
        pdb=pdb,
        mtz=mtz,
        cif=cif,
        occ_params=os.path.join(os.path.dirname(pdb), "occ.gelly"),
    )

    if not os.path.isdir(refinement_script_dir):
        os.makedirs(refinement_script_dir)

    # File location and name
    csh_file = os.path.join(
        refinement_script_dir,
        "{}_{}_{}.csh".format(crystal, refinement_program, refinement_type),
    )


    with open(csh_file, "w") as csh_f:
        csh_f.write(cmd)


def write_quick_refine_csh(
    refine_pdb,
    cif,
    free_mtz,
    crystal,
    refinement_params,
    out_dir,
    refinement_script_dir,
    column_labels=None,
    refinement_program="refmac",
    refinement_type="superposed",
    out_prefix="refine_",
    dir_prefix="refine_",
    ccp4_path=Path().ccp4,
):

    """
    Write .csh script to refine using giant.quick_refine

    Writes script to run on cluster with giant.quick_refine
    and two calls to split, with and without resetting occupancies

    Parameters
    ----------
    ccp4_path: str
        path to ccp4 setup script to be sourced
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
    pbs_line = "#PBS -joe -N \n"

    # TODO Test whether this module load is needed,
    #       as there are now no phenix dependent lines
    module_load = ""
    if os.getcwd().startswith("/dls"):
        module_load = "module load phenix\n"

    source = "source {}".format(ccp4_path)

    if not os.path.isdir(refinement_script_dir):
        os.makedirs(refinement_script_dir)

    if column_labels == None:
        args = " "
    elif refinement_program == "phenix":
        args = "input.args='xray_data.labels={}'".format(column_labels)
    elif refinement_program == "refmac":
        # Currently refmac doesn't need column labels
        args = " "
    else:
        args = " "

    # Shell suitable string for csh file
    Cmds = (
        "#!"
        + os.getenv("SHELL")
        + "\n"
        + pbs_line
        + "\n"
        + module_load
        + "\n"
        + source
        + "\n"
        + "cd {}\n".format(out_dir)
        + "giant.quick_refine"
        " input.pdb=%s" % refine_pdb
        + " mtz=%s" % free_mtz
        + " cif=%s" % cif
        + " program=%s" % refinement_program
        + " params=%s" % refinement_params
        + " dir_prefix='%s'" % dir_prefix
        + " out_prefix='%s'" % out_prefix
        + " split_conformations='False' "
        + args
        + "\n"
        "cd " + os.path.join(out_dir, dir_prefix + "0001") + "\n"
        "giant.split_conformations"
        " input.pdb='%s.pdb'" % out_prefix + " reset_occupancies=False"
        " suffix_prefix=split"
        "\n"
        "giant.split_conformations"
        " input.pdb='%s.pdb'" % out_prefix + " reset_occupancies=True"
        " suffix_prefix=output "
        "\n"
    )

    # File location and name
    csh_file = os.path.join(
        refinement_script_dir,
        "{}_{}_{}.csh".format(crystal, refinement_program, refinement_type),
    )

    # Write file
    cmd = open(csh_file, "w")
    cmd.write(Cmds)
    cmd.close()


def prepare_refinement(
    pdb,
    crystal,
    cif,
    mtz,
    ncyc,
    out_dir,
    refinement_script_dir,
    refinement_program,
    refinement_type,
    script_dir,
    ccp4_path=Path().ccp4,
):

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
    cif, params, mtz = check_inputs(
        cif=cif,
        pdb=pdb,
        params="",
        free_mtz=mtz,
        refinement_program=refinement_program,
        input_dir=input_dir,
        crystal=crystal,
        out_dir=out_dir,
    )

    column_labels = get_col_labels(out_dir=out_dir, crystal=crystal, mtz=mtz)

    # generate symlinks to refinement files
    input_cif, input_pdb, input_params, input_mtz = make_copies_and_symlinks(
        input_dir=input_dir, cif=cif, pdb=pdb, params=None, free_mtz=mtz
    )

    if refinement_program == "refmac":
        write_refmac_csh(
            pdb=input_pdb,
            crystal=crystal,
            cif=input_cif,
            mtz=input_mtz,
            out_dir=input_dir,
            refinement_script_dir=refinement_script_dir,
            script_dir=script_dir,
            ncyc=ncyc,
            ccp4_path=ccp4_path,
        )
    elif refinement_program == "buster":

        write_buster_csh(
            pdb=input_pdb,
            mtz=input_mtz,
            cif=input_cif,
            script_dir=script_dir,
            refinement_script_dir=refinement_script_dir,
            out_dir=input_dir,
            crystal=crystal,
            refinement_type="bound",
            refinement_program="buster",
            template_name="buster_template.csh",
        )

    elif refinement_program == "phenix":

        write_phenix_csh(
            pdb=input_pdb,
            mtz=input_mtz,
            cif=input_cif,
            script_dir=script_dir,
            refinement_script_dir=refinement_script_dir,
            out_dir=input_dir,
            crystal=crystal,
            ncyc=ncyc,
            column_labels=column_labels,
            refinement_type="bound",
        )


def prepare_superposed_refinement(
    crystal,
    pdb,
    cif,
    out_dir,
    refinement_script_dir,
    refinement_type="superposed",
    extra_params="NCYC=50",
    free_mtz="",
    params="",
    refinement_program="refmac",
):

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
    refinement_program
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
    cif, params, free_mtz = check_inputs(
        cif=cif,
        pdb=pdb,
        params=params,
        free_mtz=free_mtz,
        refinement_program=refinement_program,
        input_dir=input_dir,
        crystal=crystal,
        out_dir=out_dir,
    )

    # generate symlinks to refinement files
    input_cif, input_pdb, input_params, input_mtz = make_copies_and_symlinks(
        input_dir=input_dir, cif=cif, pdb=pdb, params=params, free_mtz=free_mtz
    )

    # get mtz column labels if there is a choice
    column_labels = get_col_labels(crystal=crystal, mtz=free_mtz, out_dir=out_dir)

    # Check for failed refinement due to restraint error
    # Run giant.make_restraints in this case
    if refinement_program != "exhaustive":
        if check_restraints(input_dir):
            params, _ = make_restraints(
                pdb=pdb,
                ccp4=Path().ccp4,
                refinement_program=refinement_program,
                working_dir=input_dir,
            )

    # Check that refinement is failing due to a cif file missing
    if refinement_program != "exhaustive":
        if check_refinement_for_cif_error(input_dir):
            smiles = smiles_from_crystal(crystal)
            cif_backup = os.path.join(input_dir, "backup.cif")
            os.rename(input_cif, cif_backup)
            smiles_to_cif_acedrg(smiles, input_cif)

    if refinement_program != "exhaustive":
        update_refinement_params(params=input_params, extra_params=extra_params)

    if refinement_program != "exhaustive":
        write_quick_refine_csh(
            refine_pdb=input_pdb,
            cif=input_cif,
            free_mtz=input_mtz,
            crystal=crystal,
            refinement_params=input_params,
            out_dir=input_dir,
            refinement_script_dir=refinement_script_dir,
            refinement_program=refinement_program,
            refinement_type=refinement_type,
            column_labels=column_labels,
            out_prefix="refine_1",
            dir_prefix="refine_",
        )
    else:
        write_exhaustive_csh(
            pdb=input_pdb,
            mtz=input_mtz,
            script_dir=Path().script_dir,
            refinement_script_dir=refinement_script_dir,
            out_dir=input_dir,
            crystal=crystal,
            exhaustive_multiple_sampling=Path().exhaustive_multiple_sampling,
            ccp4_path=Path().ccp4,
            refinement_type="superposed",
        )
