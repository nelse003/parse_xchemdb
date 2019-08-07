import luigi
import os
import glob
import shutil
from utils.symlink import make_symlink

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

        make_symlink(file=self.cif,
                     link_dir=os.path.join(self.out_dir, self.crystal),
                     link_name="input.cif")

        output = os.path.join(self.out_dir, self.crystal, "buster_cif_check")

        os.system("{script} {pdb} > {output}".format(script=os.path.join(Path().script_dir,
                                                            "refinement",
                                                            "check_if_buster_needs_cif.sh"),
                                                    pdb=self.pdb,
                                                    output=output))

        lig_res_names = []

        with open(output, 'r') as f:
            for line in f.readlines():
                if  "will need dictionary for residue" in line and "LIG" not in line:
                    lig_res_names.append(line.split()[-1])

        print(lig_res_names)

        out_cif = os.path.join(self.out_dir,
                               self.crystal,
                               "input.cif")

        tmp_cif = os.path.join(self.out_dir,
                               self.crystal,
                               "tmp.cif")

        if len(lig_res_names) != 0:
            os.system(
                "module load phenix;"
                "mkdir {out_dir};"
                "cd {out_dir};"
                "mkdir ./ready_set;"
                "cd ready_set;"
                "phenix.ready_set {pdb}".format(
                    out_dir=os.path.join(self.out_dir, self.crystal), pdb=self.pdb
                )
            )

            elbow_join_command = "module load phenix;elbow.join_cif_files {}".format(out_cif)

            for lig_name in lig_res_names:
                ready_set_dir=os.path.join(self.out_dir, self.crystal, "ready_set")
                lig_file = os.path.join(ready_set_dir,lig_name + ".cif")
                if os.path.exists(lig_file):
                    elbow_join_command += " {} ".format(lig_file)


            elbow_join_command += " {}".format(tmp_cif)
            os.system(elbow_join_command)
            os.remove(out_cif)
            shutil.copy(src=tmp_cif, dst=out_cif, follow_symlinks=False)

        #
        # num_cif = len(
        #     glob.glob1(os.path.join(self.out_dir, self.crystal, "ready_set"), "*.cif")
        # )
        # print(num_cif)
        #
        # if num_cif > 2:
        #     new_cif = os.path.join(
        #         self.out_dir,
        #         self.crystal,
        #         "ready_set",
        #         (os.path.basename(self.pdb)).replace(".pdb", ".ligands.cif"),
        #     )
        #     out_cif = os.path.join(self.out_dir, self.crystal, "input.cif")
        #     shutil.copy(new_cif, out_cif, follow_symlinks=False)

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
        dummy_file = os.path.join(self.out_dir,
                                  self.crystal,
                                  "dummy_file")

        return luigi.LocalTarget(dummy_file)

    def run(self):

        pdb = os.path.join(self.out_dir,
                           self.crystal,
                           "input.pdb")

        os.symlink(src=self.pdb,
                   dst=pdb)

        params, _ = make_restraints(
            pdb=pdb,
            ccp4=Path().ccp4,
            refinement_program=self.refinement_program,
            working_dir=os.path.join(self.out_dir,self.crystal),
        )
        refmac_params = os.path.join(self.out_dir,
                                     self.crystal,
                                     "multi-state-restraints.refmac.params")

        # If occupancy group contains only ligand, then
        # count_occ_group_lines = 3
        count_occ_group_lines = 0
        if os.path.exists(refmac_params):
            with open(refmac_params,'r') as refmac_param_file:
                for line in refmac_param_file:
                    if line.startswith("occupancy group"):
                       count_occ_group_lines += 1

        # If occupancy group contains only ligand
        if count_occ_group_lines <= 3:

            # Split the input pdb into bound and ground state
            os.system("cd {};giant.split_conformations {}".format(
                os.path.join(self.out_dir,self.crystal),
                pdb))

            ground_pdb = pdb.replace('.pdb','.split.ground-state.pdb')
            bound_pdb = pdb.replace('.pdb','.split.bound-state.pdb')

            # Add water atom to input.split.ground-state.pdb
            os.system("ccp4-python "
                      "/dls/science/groups/i04-1/elliot-dev/"
                      "parse_xchemdb/ccp4/copy_water_centroid.py "
                      "--bound_pdb {} "
                      "--ground_pdb {} "
                      "--output_pdb {}".format(bound_pdb, ground_pdb, ground_pdb))

            os.system("cd {};giant.merge_conformations "
                      "major={} minor={} output.pdb={} "
                      "options.minor_occupancy=0.9".format(
                os.path.join(self.out_dir,self.crystal),
                ground_pdb,
                bound_pdb,
                pdb))

        # Remove restraints from non-water case
        if os.path.exists(os.path.join(self.out_dir,
                               self.crystal,
                            "multi-state-restraints.phenix.params")):
            os.remove(os.path.join(self.out_dir,
                                   self.crystal,
                                "multi-state-restraints.phenix.params"))

        if os.path.exists(os.path.join(self.out_dir,
                               self.crystal,
                            "multi-state-restraints.refmac.params")):
            os.remove(os.path.join(self.out_dir,
                                   self.crystal,
                                "multi-state-restraints.refmac.params"))

        if os.path.exists(os.path.join(self.out_dir,
                               self.crystal,
                            "params.gelly")):

            os.remove(os.path.join(self.out_dir,
                                   self.crystal,
                                "params.gelly"))

        # Make restraints now water is added
        params, _ = make_restraints(
            pdb=pdb,
            ccp4=Path().ccp4,
            refinement_program=self.refinement_program,
            working_dir=os.path.join(self.out_dir,self.crystal),
        )

        # Link to new parameters
        if "phenix" in self.refinement_program:
            os.symlink(dst=os.path.join(self.out_dir,
                                   self.crystal,
                                        "input.params"),
                       src=os.path.join(self.out_dir,
                                   self.crystal,
                                "multi-state-restraints.phenix.params"))

        elif "refmac" in self.refinement_program:
            os.symlink(dst=os.path.join(self.out_dir,
                                   self.crystal,
                                    "input.params"),
                       src=os.path.join(self.out_dir,
                                   self.crystal,
                                "multi-state-restraints.refmac.params"))

        elif "buster" in self.refinement_program:
            os.symlink(dst=os.path.join(self.out_dir,
                                   self.crystal,
                                    "input.params"),
                       src=os.path.join(self.out_dir,
                                   self.crystal,
                                "multi-state-restraints.refmac.params"))

        dummy_file = os.path.join(self.out_dir, self.crystal, "dummy_file")

        with open(dummy_file,'w') as dummy_f:
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
