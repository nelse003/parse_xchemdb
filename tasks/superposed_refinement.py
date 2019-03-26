import os

import luigi

from path_config import Path
from refinement import prepare_superposed_refinement, convergence_state_by_refinement_type
from tasks.database import ParseXchemdbToCsv
from tasks.filesystem import RefinementFolderToCsv


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

    def requires(self):
        return ParseXchemdbToCsv()

    def output(self):

        ref_script = os.path.join(self.refinement_script_dir,
                     '{}.csh'.format(self.crystal))
        return luigi.LocalTarget(ref_script)


    def run(self):
        prepare_superposed_refinement(crystal=self.crystal,
                                      pdb=self.pdb,
                                      cif=self.cif,
                                      out_dir=self.out_dir,
                                      refinement_script_dir=self.refinement_script_dir,
                                      extra_params=self.extra_params,
                                      free_mtz=self.free_mtz)


class ConvergenceStateByRefinementType(luigi.Task):

    occ_csv = luigi.Parameter()
    occ_conv_state_csv = luigi.Parameter()
    refinement_type = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.occ_conv_state_csv)

    def requires(self):
        RefinementFolderToCsv(output_csv=self.occ_csv,
                              input_folder=Path().bound_refinement_dir),

    def run(self):
        convergence_state_by_refinement_type(occ_csv=self.occ_csv,
                                             occ_conv_state_csv=self.occ_conv_state_csv,
                                             refinement_type=self.refinement_type)