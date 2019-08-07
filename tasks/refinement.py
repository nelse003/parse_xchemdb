import os
import luigi
from luigi.util import requires
from refinement.giant_scripts import split_conformations
from refinement.prepare_scripts import prepare_refinement
from tasks.superposed_refinement import CheckRefinementIssues
from path_config import Path

@requires(CheckRefinementIssues)
class SplitConformations(luigi.Task):
    """
    Task to run giant.split conformations

    Attributes
    -----------
    input_pdb: luigi.Parameter()
        path to superposed pdb to be used as input
    working_dir: luigi.Parameter()
        path to directory to carry out split conformations

    Methods
    -------
    requires()
        reqiures ParseXchemdbToCsv(),
        existence of csv file with refinement and
        crystal details from xchemdb
    output()
        split.ground-state.pdb and split.bound-state.pdb files
        in the working directory
    run()
        wrapped version of giant.split_conformations
    """

    pdb = luigi.Parameter()
    working_dir = luigi.Parameter()
    refinement_type = luigi.Parameter()

    def output(self):

        pdb = self.pdb
        base_pdb = pdb.split(".")[0]
        out_ground_pdb = os.path.join(
            self.working_dir, "{}.split.bound-state.pdb".format(base_pdb)
        )

        out_bound_pdb = os.path.join(
            self.working_dir, "{}.split.ground-state.pdb".format(base_pdb)
        )

        return luigi.LocalTarget(out_bound_pdb), luigi.LocalTarget(out_ground_pdb)

    def run(self):
        split_conformations(pdb=self.pdb,
                            working_dir=self.working_dir,
                            refinement_type=self.refinement_type)

class PrepareRefinement(luigi.Task):
    """
    Task to generate csh for non-superposed refinement

    Attributes
    -----------
    crystal: luigi.Parameter
        crystal name

    pdb: luigi.Parameter
        path to pdb file

    cif: luigi.Parameter
        path to cif file

    out_dir: luigi.Parameter
        path to output directory

    refinement_script_dir: luigi.Parameter
        path to refinement script directory

    free_mtz: luigi.Parameter
        path to free mtz

    refinement_program: luigi.Parameter
        refinement program: exhaustive, refmac, phenix, buster

    refinement_type: luigi.Parameter
        type of refinement "bound" or "ground"

    output_csv: luigi.Parameter
        path to csv ??

    script_dir:luigi.Parameter
        path to script directory to get location of ccp4 files

    ncyc: luigi.Parameter
        number of cycles for refienement

    Methods
    -------
    requires()
        Require task which runs giant.split_conformations
    output()
        target of refinement script
        '<crystal_name>_<type>.csh'
        in refinemnt script dir. where type is either "ground" or "bound"
    run()
        runs prepare_refinement()

    """

    crystal = luigi.Parameter()
    pdb = luigi.Parameter()
    cif = luigi.Parameter()
    out_dir = luigi.Parameter()
    refinement_script_dir = luigi.Parameter()
    free_mtz = luigi.Parameter()
    refinement_type = luigi.Parameter()
    extra_params = luigi.Parameter()
    refinement_program = luigi.Parameter()
    output_csv = luigi.Parameter()
    script_dir = luigi.Parameter()
    ncyc = luigi.Parameter()

    def requires(self):

        # Set symlink to pdb location.
        # Needed to get expected working location for split conformations

        working_dir = os.path.join(self.out_dir, self.crystal)
        input_pdb = None
        if self.pdb is not None:
            if not os.path.exists(working_dir):
                os.makedirs(working_dir)

            input_pdb = os.path.join(working_dir, "input.pdb")

            if not os.path.isfile(input_pdb):
                try:
                    os.symlink(src=self.pdb, dst=input_pdb)
                except FileExistsError:
                    pass

        return SplitConformations(pdb=input_pdb,
                                  working_dir=working_dir,
                                  crystal=self.crystal,
                                  cif=self.cif,
                                  out_dir=self.out_dir,
                                  refinement_script_dir=self.refinement_script_dir,
                                  extra_params=self.extra_params,
                                  free_mtz=self.free_mtz,
                                  output_csv=self.output_csv,
                                  refinement_type=self.refinement_type,
                                  refinement_program=self.refinement_program,
                                  ),

    def output(self):

        refinement_script = os.path.join(
            self.refinement_script_dir,
            "{}_{}_{}.csh".format(
                self.crystal, self.refinement_program, self.refinement_type
            ),
        )

        return luigi.LocalTarget(refinement_script)

    def run(self):
        prepare_refinement(
            pdb=self.pdb,
            crystal=self.crystal,
            cif=self.cif,
            mtz=self.free_mtz,
            ncyc=self.ncyc,
            out_dir=self.out_dir,
            refinement_program=self.refinement_program,
            refinement_type=self.refinement_type,
            script_dir=self.script_dir,
            refinement_script_dir=self.refinement_script_dir,
            ccp4_path=Path().ccp4,
        )
