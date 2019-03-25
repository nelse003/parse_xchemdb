import luigi
from luigi.util import requires

import os
import pandas as pd
import numpy as np
import csv

from cluster_submission import submit_job
from cluster_submission import run_qstat

from parse_refmac_logs import get_occ_from_log

from annotate_ligand_state import annotate_csv_with_state_comment
from tasks.qsub import QsubSuperposedRefinement
from tasks.qsub import QsubRefinement

from refinement import prepare_superposed_refinement
from refinement_summary import refinement_summary
from refinement import get_most_recent_quick_refine
from refinement import split_conformations
from refinement import prepare_refinement
from refinement import state_occupancies
from refinement import convergence_state_by_refinement_type

# Configuration
from path_config import Path
from tasks.filesystem import RefinementFolderToCsv
from tasks.database import ParseXchemdbToCsv, RefineToCsv, SuperposedToCsv
from tasks.update_csv import OccStateComment


class BatchRefinement(luigi.Task):

    """Run a Batch of refinement jobs

    This works to set the job up, and not run if already started,
    but doesn't retry to check jobs.

    #TODO Parametrise at qsub task level so if statement aren't needed

    Methods
    -------
    requires()
        batch of QSubRefinement or QsubSuperposedRefinement Tasks
    output()
        csv path to a csv summarising failures and
        sucesses of jobs submitted to qsub

    Attributes
    -----------
    output_csv: luigi.Parameter()
        path to csv output summarise success and failure

    refinement type: luigi.Parameter()
        "ground", "bound" or "superposed" to separate out different
        refinement types

    log_pdb_mtz_csv: luigi.Parameter()
        summary csv contianing at least path to pdb, mtz
        and refinement log file

    out_dir: luigi.Parameter()
        output directory

    tmp_dir: luigi.Parameter()
        temporary directory to hold scripts

    extra_params: luigi.Parameter()
        extra parameters to provide to superposed refinement

    Notes
    ---------
    Output is only local to the current run,
    does not include previously completed jobs

    Skeleton code adapted from:

    https://github.com/xchem/formulatrix_pipe/blob/master/run_ranker.py
    """
    output_csv = luigi.Parameter()
    log_pdb_mtz_csv = luig.Paramter(default=Path().log_pdb_mtz)
    refinement_type = luigi.Parameter()
    out_dir = luigi.Parameter()
    tmp_dir = luigi.Parameter(default=Path().tmp_dir)
    extra_params = luigi.Parameter(default="NCYC=50", significant=False)

    def output(self):
        return luigi.LocalTarget(self.output_csv)

    def requires(self):
        """
        Batch of QsubRefinement tasks

        Returns
        -------
        refinement_tasks: list of Luigi.Tasks
        """

        # Read crystal/refinement table csv
        df = pd.read_csv(self.log_pdb_mtz_csv)

        # Replace Nans with empty strings,
        # used to allow luigi.Parameters
        df = df.replace(np.nan, '', regex=True)

        # Loop over crystal/refienemnt table csv
        refinement_tasks = []
        for i in df.index:
            cif = df.at[i, 'cif']
            pdb = df.at[i, 'pdb_latest']
            mtz = df.at[i, 'mtz_free']
            crystal = df.at[i, 'crystal_name']

            # Cheat to allow run on single folder
            # if crystal != "FIH-x0439":
            #     continue

            refinement_script = os.path.join(self.tmp_dir,
                                             "{}_{}.csh".format(crystal, self.refinement_type))
            # produce refinement task
            if self.refinement_type in ["bound","ground"]:

                ref_task = QsubRefinement(
                                refinement_script=refinement_script,
                                crystal=crystal,
                                pdb=pdb,
                                cif=cif,
                                free_mtz=mtz,
                                refinement_script_dir=self.tmp_dir,
                                out_dir=self.out_dir,
                                refinement_type=self.refinement_type,
                                output_csv=self.output_csv)

            elif self.refinement_type == "superposed":

                ref_task = QsubSuperposedRefinement(
                            refinement_script=refinement_script,
                            crystal=crystal,
                            pdb=pdb,
                            cif=cif,
                            free_mtz=mtz,
                            refinement_script_dir=self.tmp_dir,
                            extra_params=self.extra_params,
                            out_dir=Path().refinement_dir,
                            output_csv=self.output_csv)

            # add to list of refienement tasks
            refinement_tasks.append(ref_task)

        return refinement_tasks


class SummaryRefinement(luigi.Task):

    """
    Task to summarise refinement in csv

    Methods
    --------
    requires()
        Occupancy convergence csv,
        refinement table csv,
        refinement that have valid pdbs csv,
        csv file with refinement and crystal details from xchemdb
    output()
        path to refinement summary csv
    run()
        generate renfiment summary csv
    """

    def requires(self):
        return OccStateComment(log_labelled_csv=Path().log_occ_resname,
                               occ_conv_csv=Path().occ_conv,
                               log_pdb_mtz=Path().log_pdb_mtz), \
               ParseXchemdbToCsv(log_pdb_mtz_csv=Path().log_pdb_mtz), \
               SuperposedToCsv(), \
               RefineToCsv()

    def output(self):
        return luigi.LocalTarget(Path().refinement_summary)
    def run(self):
        refinement_summary(occ_conv_csv=Path().occ_conv,
                           refine_csv=Path().refine,
                           superposed_csv=Path().superposed,
                           log_pdb_mtz_csv=Path().log_pdb_mtz,
                           out_csv=Path().refinement_summary)


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

        ref_script = os.path.join(self.refinement_script_dir,
                     '{}_{}.csh'.format(self.crystal, self.refinement_type))

        return luigi.LocalTarget(ref_script)

    def run(self):
        prepare_refinement(pdb=self.pdb,
                           crystal=self.crystal,
                           cif=self.cif,
                           mtz=self.free_mtz,
                           ncyc=50,
                           out_dir=self.out_dir,
                           refinement_script_dir=self.refinement_script_dir,
                           ccp4_path="/dls/science/groups/i04-1/" \
                             "software/pandda_0.2.12/ccp4/ccp4-7.0/bin/" \
                             "ccp4.setup-sh")

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


@PrepareSuperposedRefinement.event_handler(luigi.Event.FAILURE)
@QsubSuperposedRefinement.event_handler(luigi.Event.FAILURE)
@PrepareRefinement.event_handler(luigi.Event.FAILURE)
@QsubRefinement.event_handler(luigi.Event.FAILURE)
def failure_write_to_csv(task, exception):
    """
    If failure of task occurs, summarise in CSV

    Parameters
    ----------
    task: luigi.Task
        task for which this post failure function will be run

    Returns
    -------
    None
    """

    with open(task.output_csv, 'a') as task_csv:
        task_csv_writer = csv.writer(task_csv, delimiter=',')
        task_csv_writer.writerow(["Failure", task.crystal, type(exception), exception])

@PrepareSuperposedRefinement.event_handler(luigi.Event.SUCCESS)
@QsubSuperposedRefinement.event_handler(luigi.Event.SUCCESS)
@PrepareRefinement.event_handler(luigi.Event.SUCCESS)
@QsubRefinement.event_handler(luigi.Event.SUCCESS)
def success_write_to_csv(task):
    """
    If success of task occurs, summarise in CSV

    Parameters
    ----------
    task: luigi.Task
        task for which this post sucess function will be run

    Returns
    -------
    None
    """

    with open(task.output_csv, 'a') as task_csv:
        task_csv_writer = csv.writer(task_csv, delimiter=',')
        task_csv_writer.writerow(["Sucesss", task.crystal])


class StateOccupancyToCsv(luigi.Task):

    """
    Add convergence summary and sum ground and bound state occupancies to csv

    Adds convergence ratio
    x(n)/(x(n-1) -1)
    to csv.

    Adds up occupancy for ground and bound states respectively
    across each complete group

    Methods
    -------
    run()
        refinement.state_occupancies()
    requires()
        OccConvergence() to get lead in csv
    output()
        csv file path
    """

    occ_correct_csv = luigi.Parameter()
    occ_conv_csv = luigi.Parameter()
    log_occ_resname = luigi.Parameter()
    log_pdb_mtz = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.occ_correct_csv)

    def requires(self):
        OccStateComment(log_occ_resname=self.log_labelled_csv,
                        occ_conv_csv=self.occ_conv_csv,
                        log_pdb_mtz=self.log_pdb_mtz)

    def run(self):
        state_occupancies(occ_conv_csv=self.occ_conv_csv,
                          occ_correct_csv=self.occ_correct_csv)


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
