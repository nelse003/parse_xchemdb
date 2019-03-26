import os

import luigi

from refinement import split_conformations, prepare_refinement


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

    def output(self):

        pdb = self.pdb
        base_pdb = pdb.split('.')[0]
        out_ground_pdb = os.path.join(self.working_dir,
                                      "{}.split.bound-state.pdb".format(base_pdb))

        out_bound_pdb = os.path.join(self.working_dir,
                                     "{}.split.ground-state.pdb".format(base_pdb))

        return luigi.LocalTarget(out_bound_pdb), luigi.LocalTarget(out_ground_pdb)

    def run(self):
        split_conformations(pdb=self.pdb, working_dir=self.working_dir)


class PrepareRefinement(luigi.Task):
    """
    Task to generate csh for non-superposed refinement

    Attributes
    -----------


    Methods
    -------
    requires()

    output()
        target of refinement script
        '<crystal_name>_<type>.csh'
        in refinemnt script dir. where type is either "ground" or "bound"
    run()

    """

    crystal = luigi.Parameter()
    pdb = luigi.Parameter()
    cif = luigi.Parameter()
    out_dir = luigi.Parameter()
    refinement_script_dir = luigi.Parameter()
    free_mtz = luigi.Parameter()
    refinement_type = luigi.Parameter()
    output_csv = luigi.Parameter()
    script_dir = luigi.Parameter()

    def requires(self):

        # Set symlink to pdb location.
        # Needed to get expected working location for split conformations

        working_dir = os.path.join(self.out_dir, self.crystal)
        input_pdb = None
        if self.pdb is not None:
            if not os.path.exists(working_dir):
                os.makedirs(working_dir)

            input_pdb = os.path.join(working_dir, "input.pdb")
            if not os.path.exists(input_pdb):
                os.symlink(self.pdb, input_pdb)

        return SplitConformations(pdb=input_pdb, working_dir=working_dir)

    def output(self):

        refinement_script = os.path.join(self.refinement_script_dir,
                     '{}_{}.csh'.format(self.crystal, self.refinement_type))

        return luigi.LocalTarget(refinement_script)

    def run(self):
        prepare_refinement(pdb=self.pdb,
                           crystal=self.crystal,
                           cif=self.cif,
                           mtz=self.free_mtz,
                           ncyc=50,
                           out_dir=self.out_dir,
                           refinement_type=self.refinement_type,
                           script_dir=self.script_dir,
                           refinement_script_dir=self.refinement_script_dir,
                           ccp4_path="/dls/science/groups/i04-1/" \
                             "software/pandda_0.2.12/ccp4/ccp4-7.0/bin/" \
                             "ccp4.setup-sh")