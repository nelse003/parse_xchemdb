import os
import luigi
import time
from luigi.util import requires

from utils.cluster_submission import run_qstat, submit_job
from path_config import Path

import tasks.refinement
import tasks.superposed_refinement

class QsubTask(luigi.Task):
    """
    Base class for Qsub Tasks
    """
    submitted = False

    def get_queue_jobs(self):

        # run 'qstat -r'
        output_queue = run_qstat()

        queue_jobs = []
        # Turn qstat output into list of jobs
        for line in output_queue:
            if "Full jobname" in line:
                jobname = line.split()[-1]
                queue_jobs.append(jobname)

        return queue_jobs

class QsubEdstats(QsubTask):
    """
    Task for submission of Edstats
    """
    pdb = luigi.Parameter()
    mtz = luigi.Parameter()
    out_dir = luigi.Parameter()
    ccp4 = luigi.Parameter()

    def output(self):
        output_csv = os.path.join(self.out_dir, "residue_scores.csv")
        return luigi.LocalTarget(output_csv)

    def run(self):

        """
        Run giant.score model via qsub

        Returns
        -------

        """

        if not os.path.isfile(os.path.join(self.out_dir, "residue_scores.csv")):

            queue_jobs = self.get_queue_jobs()
            xtal = self.out_dir.split('/')[-1]
            program = self.out_dir.split('/')[-2]
            job = os.path.join(self.out_dir,
                               "{xtal}_{program}"
                               "_score_model.csh".format(xtal=xtal,
                                                       program=program))
            print(xtal)
            print(program)
            with open(job,'w') as f:

                f.write("source {}\n".format(self.ccp4))
                f.write("giant.score_model {pdb} {mtz} "
                        "output.out_dir={out_dir}".format(pdb=self.pdb,
                                                          mtz=self.mtz,
                                                          out_dir=self.out_dir))

            # Get csh filename
            job_file = os.path.basename(str(job))

            # Check whether <crystal_name>.csh is running in queue,
            # If not submit job to queue
            if job_file not in queue_jobs and not self.submitted:
                submit_job(job_directory=self.out_dir, job_script=job_file)

                # If the job has already been submitted then change flag
                # This means recursion only loops until job is finished
                # and does not resubmit
                self.submitted = True

                print(
                    "The job had no output, and was not found to be running "
                    "in the queue. The job has been submitted. "
                )

            # Run until job complete
            time.sleep(5)
            queue_jobs = self.get_queue_jobs()
            if job_file in queue_jobs:
                time.sleep(30)
                self.run()

class QsubRefinementTask(QsubTask):

    """ Base class for single job on cluster submitted by qsub

    Attributes
    -----------

    Methods
    ---------
    output()
        Task should output refined pdb and mtz files

    run()
        Check for presence of PDB and MTZ,
        if they do not exist, check for running jobs on cluster.
        If no running jobs, submit <crystal_name>.csh as job.

    Notes
    ---------
    Requires the refinement script name to the name of the crystal,
    and that the pdb/mtz are stored hierarchically in that folder.
    Uses luigi.Parameter to pass a refinement script name.

    Skeleton code adapted from working version for the formulatrix
    pipeline at diamond:

    https://github.com/xchem/formulatrix_pipe/blob/master/run_ranker.py

    """

    refinement_script = luigi.Parameter()

    def output(self):

        out_mtz = os.path.join(self.out_dir, "refine.mtz")
        out_pdb = os.path.join(self.out_dir, "refine.pdb")

        return luigi.LocalTarget(out_pdb), luigi.LocalTarget(out_mtz)

    def run(self):
        """
        Run Qsub Refienement

        Check for presence of PDB and MTZ,
        if they do not exist, check for running jobs on cluster.
        If no running jobs, submit job.

        Returns
        -------
        None

        Raises
        -------
        RuntimeError
            If the job has not been newly submitted and is not running
        """

        out_mtz = os.path.join(self.out_dir, self.crystal, "refine.mtz")
        out_pdb = os.path.join(self.out_dir, self.crystal, "refine.pdb")

        # Only run if the pdb and mtz are not present
        if not (os.path.isfile(out_pdb) and os.path.isfile(out_mtz)):

            queue_jobs = self.get_queue_jobs()

            # Get <crystal_name>_<refinement_type>.csh
            job = self.refinement_script
            job_file = os.path.basename(str(job))

            # Check whether <crystal_name>.csh is running in queue,
            # If not submit job to queue
            if job_file not in queue_jobs and not self.submitted:
                submit_job(job_directory=Path().tmp_dir, job_script=job_file)

                # If the job has already been submitted then change flag
                # This means recursion only loops until job is finished
                # and does not resubmit
                self.submitted = True

                print(
                    "The job had no output, and was not found to be running "
                    "in the queue. The job has been submitted. "
                )

            # Run until job complete
            time.sleep(5)
            queue_jobs = self.get_queue_jobs()
            if job_file in queue_jobs:
                time.sleep(30)
                self.run()


@requires(tasks.refinement.PrepareRefinement)
class QsubRefinement(QsubRefinementTask):
    pass


@requires(tasks.superposed_refinement.PrepareSuperposedRefinement)
class QsubSuperposedRefinement(QsubRefinementTask):
    pass
