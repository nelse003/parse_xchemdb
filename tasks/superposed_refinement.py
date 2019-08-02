import luigi
import os
import glob
import shutil

from refinement.prepare_scripts import prepare_superposed_refinement
from luigi.util import requires


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

        num_cif = len(
            glob.glob1(os.path.join(self.out_dir, self.crystal, "ready_set"), "*.cif")
        )
        print(num_cif)

        if num_cif > 2:
            new_cif = os.path.join(
                self.out_dir,
                self.crystal,
                "ready_set",
                (os.path.basename(self.pdb)).replace(".pdb", ".ligands.cif"),
            )
            out_cif = os.path.join(self.out_dir, self.crystal, "input.cif")
            shutil.copy(new_cif, out_cif, follow_symlinks=False)

        with open(os.path.join(self.out_dir, self.crystal, "check_file"), "w") as f:
            f.write("{}\n{}\n{}".format(self.crystal, type(self.crystal), self.out_dir))


@requires(CheckRefinementIssues)
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
