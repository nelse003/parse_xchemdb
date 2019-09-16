import luigi
import os
import glob
import shutil
import pathlib
import copy

from utils.symlink import make_symlink
from utils.filesystem import cif_path
from refinement.merge_cif import merge_cif
from refinement.prepare_scripts import prepare_superposed_refinement
from luigi.util import requires
from refinement.giant_scripts import make_restraints
from path_config import Path


class CheckRefinementIssues(luigi.Task):
    """Task to check issues with data supplied for refinement

    Atrributes
    ------------
    crystal: luigi.Parameter
        crystal name

    pdb: luigi.Parameter
        path to pdb file

    cif: luigi.Parameter
        path to cif file

    out_dir: luigi.Parameter
        path to refinement folder

    refinement_script_dir: luigi.Parameter
        path to refinement script dir to store '<crystal_name>.csh'

    extra_params: luigi.Parameter
        parameters to add to refinement.
        i.e. to run longer till convergence

    free_mtz: luigi.Parameter
        path to free mtz file

    refinement_type: luigi.Parameter
        superposed, bound or ground

    refinement_program: luigi.Parameter
        buster, phenix, refmac, exhaustive

    Notes
    ------
    PDK2 cif for buster is missing TF3: Run phenix readyset and change input cif
    """

    crystal = luigi.Parameter()
    pdb = luigi.Parameter()
    cif = luigi.Parameter()
    out_dir = luigi.Parameter()
    refinement_script_dir = luigi.Parameter()
    extra_params = luigi.Parameter()
    free_mtz = luigi.Parameter()
    output_csv = luigi.Parameter()
    refinement_type = luigi.Parameter()
    refinement_program = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.out_dir, self.crystal, "check_file"))

    def run(self):

        if not os.path.exists(os.path.join(self.out_dir, self.crystal)):
            os.makedirs(os.path.join(self.out_dir, self.crystal))

        if os.path.exists(self.cif):
            make_symlink(
                file=self.cif,
                link_dir=os.path.join(self.out_dir, self.crystal),
                link_name="input.cif",
            )

        output = os.path.join(self.out_dir, self.crystal, "buster_cif_check")

        os.system(
            "{script} {pdb} > {output}".format(
                script=os.path.join(
                    Path().script_dir, "refinement", "check_if_buster_needs_cif.sh"
                ),
                pdb=self.pdb,
                output=output,
            )
        )

        lig_res_names = []

        with open(output, "r") as f:
            for line in f.readlines():
                if "will need dictionary for residue" in line and "LIG" not in line:
                    lig_res_names.append(line.split()[-1])

        print(lig_res_names)

        out_cif = os.path.join(self.out_dir, self.crystal, "input.cif")

        print(
            "0 {}".format(
                os.path.isfile(os.path.join(self.out_dir, self.crystal, "tmp.cif"))
            )
        )

        tmp_cif = os.path.join(self.out_dir, self.crystal, "tmp.cif")

        # Check for ligand code in refmac library
        if "refmac" in self.refinement_program and len(lig_res_names) != 0:

            # http://www.ccp4.ac.uk/html/refmac5/dictionary/list-of-ligands.html
            print(Path().ccp4)
            print(Path().ccp4.rstrip("/bin/ccp4.setup-sh"))
            mon_lib = os.path.join(
                Path().ccp4.rstrip("/bin/ccp4.setup-sh"),
                "lib/data/monomers/mon_lib_ind.cif",
            )

            with open(mon_lib, "r") as monomer_library:
                monomer_library_text = monomer_library.read()

            if all(lig_name in monomer_library_text for lig_name in lig_res_names):
                pass
            else:
                if lig_name in monomer_library_text:
                    lig_res_names.remove(lig_name)

        # check existing folder for suitable cif files
        elif len(lig_res_names) != 0:

            # get initial model dir
            if os.path.exists(os.path.dirname(self.free_mtz)):
                initial_model_crystal_dir = os.path.dirname(self.free_mtz)
            else:
                print(pathlib.Path(self.pdb).resolve())
                pdb_path_dir = os.path.dirname(pathlib.Path(self.pdb).resolve())
                if "Refine" in pdb_path_dir:
                    initial_model_crystal_dir = os.path.dirname(pdb_path_dir)
                    print("initial_model_crystal_dir")
                    print(initial_model_crystal_dir)
                else:
                    initial_model_crystal_dir = pdb_path_dir

            os.chdir(initial_model_crystal_dir)

            all_lig_res_names = ["LIG"] + lig_res_names

            # get path to current cif if it doesn't contain path
            if not os.path.exists(self.cif):
                if os.path.exists(
                    os.path.join(initial_model_crystal_dir, os.path.basename(self.cif))
                ):
                    self.cif = os.path.join(
                        initial_model_crystal_dir, os.path.basename(self.cif)
                    )

            # Check_current cif file for both ligands
            with open(self.cif) as cif_f:
                file_txt = cif_f.read()

            if all(lig_name in file_txt for lig_name in all_lig_res_names):
                print("Using {}".format(self.cif))
                os.symlink(src=self.cif, dst=tmp_cif)
                os.remove(out_cif)
                shutil.copy(src=tmp_cif, dst=out_cif, follow_symlinks=False)

            elif not os.path.isfile(tmp_cif):
                # Check all other cif files in initial model folder
                lig_dict = {}
                for cif in glob.glob("*.cif"):
                    print(cif)
                    with open(cif) as cif_f:
                        file_txt = cif_f.read()

                    if all(lig_name in file_txt for lig_name in all_lig_res_names):
                        print(
                            "Using {}".format(
                                os.path.join(initial_model_crystal_dir, cif)
                            )
                        )
                        os.symlink(
                            src=os.path.join(initial_model_crystal_dir, cif),
                            dst=tmp_cif,
                        )
                        os.remove(out_cif)
                        shutil.copy(src=tmp_cif, dst=out_cif, follow_symlinks=False)
                        break

                    elif any(lig_name in file_txt for lig_name in all_lig_res_names):
                        for lig_name in all_lig_res_names:
                            if lig_name in file_txt:
                                lig_dict[lig_name] = os.path.join(
                                    initial_model_crystal_dir, cif
                                )
                    else:
                        print(
                            "Skipping {}".format(
                                os.path.join(initial_model_crystal_dir, cif)
                            )
                        )

                print("lig_dict")
                print(lig_dict)

                # if all ligands have been found by matching
                if len(lig_dict) == len(all_lig_res_names):
                    cmd = "module load phenix;elbow.join_cif_files"
                    for lig_name, cif in lig_dict:
                        cmd += " {} ".format(cif)
                    print(cmd)
                    os.system(cmd)

                else:

                    # use phenix based tools to generate cif
                    os.system(
                        "module load phenix;"
                        "mkdir {out_dir};"
                        "cd {out_dir};"
                        "mkdir ./ready_set;"
                        "cd ready_set;"
                        "phenix.ready_set {pdb}".format(
                            out_dir=os.path.join(self.out_dir, self.crystal),
                            pdb=self.pdb,
                        )
                    )

                    elbow_join_command = "module load phenix;elbow.join_cif_files"

                    ready_set_dir = os.path.join(
                        self.out_dir, self.crystal, "ready_set"
                    )

                    lig_files = []
                    for lig_name in all_lig_res_names:
                        # first get ligands from ligands known to have existing cif files
                        if lig_name in lig_dict.keys():
                            lig_file = lig_dict[lig_name]
                            lig_files.append(lig_file)
                        # then get any reamining from phenix.ready_set
                        else:
                            lig_file = os.path.join(ready_set_dir, lig_name + ".cif")
                            lig_files.append(lig_file)

                        if os.path.exists(lig_file):
                            elbow_join_command += " {} ".format(lig_file)

                    elbow_join_command += " {}".format(tmp_cif)

                    print(elbow_join_command)

                    try:
                        subprocess(elbow_join_command)
                    except:
                        print("Elbow failed. Trying Libcheck")

                        # add cif files together in sets of two
                        # using libcheck
                        while len(lig_files) != 0:
                            lig_1 = lig_files[0]
                            lig_2 = lig_files[1]
                            merge_cif(cif1=lig_1, cif2=lig_2, cif_out=tmp_cif)
                            lig_files.remove(lig_1)
                            lig_files.remove(lig_2)
                            if len(lig_files) != 0:
                                lig_files.insert(0, tmp_cif)

                        # libcheck produce a .lib file
                        # copy to .cif instead
                        # removing .lib file
                        shutil.copy(src=tmp_cif + ".lib", dst=tmp_cif)
                        os.remove(tmp_cif + ".lib")

                    # copy cif file to output cif
                    if os.path.exists(out_cif):
                        os.remove(out_cif)
                    shutil.copy(src=tmp_cif, dst=out_cif, follow_symlinks=False)

        with open(os.path.join(self.out_dir, self.crystal, "check_file"), "w") as f:

            f.write("{}\n{}\n{}".format(self.crystal, type(self.crystal), self.out_dir))


@requires(CheckRefinementIssues)
class AddDummyWater(luigi.Task):

    """
    Task to add water atoms to centroid of ligand

    Check whether occupancy group is just ligand.
    If the occupancy group contains other stuff,
    return without changing the files to be refined.
    Else add a water to the centre of ligand location
    in the ground state.

    Atrributes
    ------------
    crystal: luigi.Parameter
        crystal name

    pdb: luigi.Parameter
        path to pdb file

    cif: luigi.Parameter
        path to cif file

    out_dir: luigi.Parameter
        path to refinement folder

    refinement_script_dir: luigi.Parameter
        path to refinement script dir to store '<crystal_name>.csh'

    extra_params: luigi.Parameter
        parameters to add to refinement.
        i.e. to run longer till convergence

    free_mtz: luigi.Parameter
        path to free mtz file

    refinement_type: luigi.Parameter
        superposed, bound or ground

    refinement_program: luigi.Parameter
        buster, phenix, refmac, exhaustive
    """

    def output(self):
        dummy_file = os.path.join(self.out_dir, self.crystal, "dummy_file")

        return luigi.LocalTarget(dummy_file)

    def run(self):

        dummy_file = os.path.join(self.out_dir, self.crystal, "dummy_file")

        pdb = os.path.join(self.out_dir, self.crystal, "input.pdb")

        os.symlink(src=self.pdb, dst=pdb)

        params, _ = make_restraints(
            pdb=pdb,
            ccp4=Path().ccp4,
            refinement_program=self.refinement_program,
            working_dir=os.path.join(self.out_dir, self.crystal),
        )
        refmac_params = os.path.join(
            self.out_dir, self.crystal, "multi-state-restraints.refmac.params"
        )

        # If occupancy group contains only ligand, then
        # count_occ_group_lines = 3
        count_occ_group_lines = 0
        if os.path.exists(refmac_params):
            with open(refmac_params, "r") as refmac_param_file:
                for line in refmac_param_file:
                    if line.startswith("occupancy group"):
                        count_occ_group_lines += 1

        # If occupancy group contains only ligand
        if count_occ_group_lines <= 3:

            # Split the input pdb into bound and ground state
            os.system(
                "cd {};giant.split_conformations {}".format(
                    os.path.join(self.out_dir, self.crystal), pdb
                )
            )

            ground_pdb = pdb.replace(".pdb", ".split.ground-state.pdb")
            bound_pdb = pdb.replace(".pdb", ".split.bound-state.pdb")

            # Add water atom to input.split.ground-state.pdb
            os.system(
                "ccp4-python "
                "/dls/science/groups/i04-1/elliot-dev/"
                "parse_xchemdb/ccp4/copy_water_centroid.py "
                "--bound_pdb {} "
                "--ground_pdb {} "
                "--output_pdb {}".format(bound_pdb, ground_pdb, ground_pdb)
            )

            os.system(
                "cd {};giant.merge_conformations "
                "major={} minor={} output.pdb={} "
                "options.minor_occupancy=0.9".format(
                    os.path.join(self.out_dir, self.crystal), ground_pdb, bound_pdb, pdb
                )
            )

        # Remove restraints from non-water case
        if os.path.exists(
            os.path.join(
                self.out_dir, self.crystal, "multi-state-restraints.phenix.params"
            )
        ):
            os.remove(
                os.path.join(
                    self.out_dir, self.crystal, "multi-state-restraints.phenix.params"
                )
            )

        if os.path.exists(
            os.path.join(
                self.out_dir, self.crystal, "multi-state-restraints.refmac.params"
            )
        ):
            os.remove(
                os.path.join(
                    self.out_dir, self.crystal, "multi-state-restraints.refmac.params"
                )
            )

        if os.path.exists(os.path.join(self.out_dir, self.crystal, "params.gelly")):

            os.remove(os.path.join(self.out_dir, self.crystal, "params.gelly"))

        # Make restraints now water is added
        params, _ = make_restraints(
            pdb=pdb,
            ccp4=Path().ccp4,
            refinement_program=self.refinement_program,
            working_dir=os.path.join(self.out_dir, self.crystal),
        )

        # Link to new parameters
        if "phenix" in self.refinement_program:
            os.symlink(
                dst=os.path.join(self.out_dir, self.crystal, "input.params"),
                src=os.path.join(
                    self.out_dir, self.crystal, "multi-state-restraints.phenix.params"
                ),
            )

        elif "refmac" in self.refinement_program:
            os.symlink(
                dst=os.path.join(self.out_dir, self.crystal, "input.params"),
                src=os.path.join(
                    self.out_dir, self.crystal, "multi-state-restraints.refmac.params"
                ),
            )

        elif "buster" in self.refinement_program:
            os.symlink(
                dst=os.path.join(self.out_dir, self.crystal, "input.params"),
                src=os.path.join(self.out_dir, self.crystal, "params.gelly"),
            )

        with open(dummy_file, "w") as dummy_f:
            if count_occ_group_lines <= 3:
                dummy_f.write("dummy atoms added")
            else:
                dummy_f.write("No dummy atoms added")


@requires(AddDummyWater)
class PrepareSuperposedRefinement(luigi.Task):

    """
    Task to generate csh file for refinement submission

    Attributes
    -----------
    crystal: luigi.Parameter
        crystal name

    pdb: luigi.Parameter
        path to pdb file

    cif: luigi.Parameter
        path to cif file

    out_dir: luigi.Parameter
        path to refinement folder

    refinement_script_dir: luigi.Parameter
        path to refinement script dir to store '<crystal_name>.csh'

    extra_params: luigi.Parameter
        parameters to add to refinement.
        i.e. to run longer till convergence

    free_mtz: luigi.Parameter
        path to free mtz file

    Methods
    --------
    requires()
        reqiures ParseXchemdbToCsv(),
        existence of csv file with refinement and
        crystal details from xchemdb
    output()
        target of refinement script
        '<crystal_name>.csh'
        in refinemnt script dir
    run()
        runs refinement.prepare_refinement()

    Notes
    -----
    Uses luigi.Parameters do the task can be parameterised and run
    many times

    """

    crystal = luigi.Parameter()
    pdb = luigi.Parameter()
    cif = luigi.Parameter()
    out_dir = luigi.Parameter()
    refinement_script_dir = luigi.Parameter()
    extra_params = luigi.Parameter()
    free_mtz = luigi.Parameter()
    output_csv = luigi.Parameter()
    refinement_script = luigi.Parameter()
    refinement_type = luigi.Parameter()
    refinement_program = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.refinement_script)

    def run(self):
        prepare_superposed_refinement(
            crystal=self.crystal,
            pdb=self.pdb,
            cif=self.cif,
            out_dir=self.out_dir,
            refinement_script_dir=self.refinement_script_dir,
            refinement_type=self.refinement_type,
            extra_params=self.extra_params,
            free_mtz=self.free_mtz,
            refinement_program=self.refinement_program,
        )
