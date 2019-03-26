import csv
import os

import luigi
import numpy as np
import pandas as pd

from path_config import Path

import tasks.refinement
import tasks.superposed_refinement
import tasks.qsub

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
    log_pdb_mtz_csv = luigi.Parameter(default=Path().log_pdb_mtz)
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
                                             '{}_{}.csh'.format(crystal,
                                                                self.refinement_type))

            # produce refinement task
            if self.refinement_type in ["bound","ground"]:

                ref_task = tasks.qsub.QsubRefinement(
                                crystal=crystal,
                                pdb=pdb,
                                cif=cif,
                                free_mtz=mtz,
                                refinement_script=refinement_script,
                                refinement_script_dir=self.tmp_dir,
                                out_dir=self.out_dir,
                                refinement_type=self.refinement_type,
                                output_csv=self.output_csv)

            elif self.refinement_type == "superposed":

                ref_task = tasks.qsub.QsubSuperposedRefinement(
                            crystal=crystal,
                            pdb=pdb,
                            cif=cif,
                            free_mtz=mtz,
                            refinement_script=refinement_script,
                            refinement_script_dir=self.tmp_dir,
                            extra_params=self.extra_params,
                            out_dir=Path().refinement_dir,
                            refinement_type="superposed",
                            output_csv=self.output_csv)

            # add to list of refienement tasks
            refinement_tasks.append(ref_task)

        return refinement_tasks


@tasks.superposed_refinement.PrepareSuperposedRefinement.event_handler(luigi.Event.FAILURE)
@tasks.qsub.QsubSuperposedRefinement.event_handler(luigi.Event.FAILURE)
@tasks.refinement.PrepareRefinement.event_handler(luigi.Event.FAILURE)
@tasks.qsub.QsubRefinement.event_handler(luigi.Event.FAILURE)
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


@tasks.superposed_refinement.PrepareSuperposedRefinement.event_handler(luigi.Event.SUCCESS)
@tasks.qsub.QsubSuperposedRefinement.event_handler(luigi.Event.SUCCESS)
@tasks.refinement.PrepareRefinement.event_handler(luigi.Event.SUCCESS)
@tasks.qsub.QsubRefinement.event_handler(luigi.Event.SUCCESS)
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