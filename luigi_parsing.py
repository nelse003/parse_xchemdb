import luigi
import os
import pandas as pd
import numpy as np
import glob
import subprocess
import csv
import time

from cluster_submission import submit_job
from parse_xchemdb import process_refined_crystals
from parse_xchemdb import get_table_df
from parse_xchemdb import drop_only_dimple_processing
from parse_xchemdb import drop_pdb_not_in_filesystem
from convergence import get_occ_from_log
from convergence import main as convergence
from plotting import main as plot_occ
from plotting import refinement_summary_plot
from refinement import prepare_refinement
from refinement_summary import refinement_summary

# Config
class Path(luigi.Config):

    """Config: all paths to be used in luigi. Tasks as parameters"""

    script_dir = "/dls/science/groups/i04-1/elliot-dev/parse_xchemdb"
    out_dir = "/dls/science/groups/i04-1/elliot-dev/Work/" \
              "exhaustive_parse_xchem_db/"
    tmp_dir = os.path.join(out_dir, "tmp")
    refinement_dir = os.path.join(out_dir, "convergence_refinement")

    # CSVS
    log_pdb_mtz = luigi.Parameter(
        default=os.path.join(out_dir, 'log_pdb_mtz.csv'))
    log_occ = luigi.Parameter(
        default=os.path.join(out_dir, 'log_occ.csv'))
    log_occ_resname = luigi.Parameter(
        default=os.path.join(out_dir, 'log_occ_resname.csv'))

    occ_conv = luigi.Parameter(
        default=os.path.join(out_dir, 'occ_conv.csv'))
    refinement_summary = luigi.Parameter(
        default=os.path.join(out_dir, 'refinement_summary.csv'))
    refine = luigi.Parameter(
        default=os.path.join(out_dir, 'refinement.csv'))
    superposed = luigi.Parameter(
        default=os.path.join(out_dir, 'superposed.csv'))
    occ_conv_failures = luigi.Parameter(
        default=os.path.join(out_dir, 'occ_conv_failures.csv'))

    convergence_refinement_failures = luigi.Parameter(
        default=os.path.join(out_dir, 'convergence_refinement.csv'))

    # Plots
    refinement_summary_plot = luigi.Parameter(
        default=os.path.join(out_dir, 'refinement_summary.png'))
    bound_occ_hist = luigi.Parameter(
        default=os.path.join(out_dir, 'bound_occ_hist.png'))

    # Scripts
    convergence_py = luigi.Parameter(
        default=os.path.join(script_dir, "convergence.py"))

    # Batch Management
    refmac_batch = luigi.Parameter(default = os.path.join(out_dir, "refmac_batch.log"))
    prepare_batch = luigi.Parameter(default=os.path.join(out_dir, "prepare_batch.log"))

    # Dirs
    script_dir = luigi.Parameter(default= script_dir)
    tmp_dir = luigi.Parameter(default= tmp_dir)
    out_dir = luigi.Parameter(default=out_dir)
    refinement_dir = luigi.Parameter(default=refinement_dir)

## Tasks ###

class ParseXchemdbToCsv(luigi.Task):
    """Parse the XChem database and turn """

    def requires(self):
        return None
    def output(self):
        return luigi.LocalTarget(Path().log_pdb_mtz)
    def run(self):
        process_refined_crystals()

class OccFromLog(luigi.Task):
    def requires(self):
        return ParseXchemdbToCsv()
    def output(self):
        return luigi.LocalTarget(Path().log_occ)
    def run(self):
        get_occ_from_log(log_pdb_mtz_csv=Path().log_pdb_mtz,
                         log_occ_csv=Path().log_occ)

class RefineToDF(luigi.Task):
    def requires(self):
        return None
    def output(self):
        return luigi.LocalTarget(Path().refine)
    def run(self):
        refine_df = get_table_df('refinement')
        refine_df.to_csv(Path().refine)

class SuperposedToDF(luigi.Task):
    def requires(self):
        return RefineToDF()
    def output(self):
        return luigi.LocalTarget(Path().superposed)
    def run(self):
        refine_df =pd.read_csv(Path().refine)
        pdb_df = refine_df[refine_df.pdb_latest.notnull()]
        pdb_df = drop_pdb_not_in_filesystem(pdb_df)
        superposed_df = drop_only_dimple_processing(pdb_df)
        superposed_df.to_csv(Path().superposed)
        pass

class SummaryRefinement(luigi.Task):
    def requires(self):
        return OccConvergence(), ParseXchemdbToCsv(), SuperposedToDF(), RefineToDF()
               #OccConvergenceFailureDF()
    def output(self):
        return luigi.LocalTarget(Path().refinement_summary)
    def run(self):
        refinement_summary(occ_conv_csv=Path().occ_conv,
                           refine_csv=Path().refine,
                           superposed_csv=Path().superposed,
                           occ_conv_failures_csv=Path().occ_conv_failures,
                           log_pdb_mtz_csv=Path().log_pdb_mtz,
                           out_csv=Path().refinement_summary)

class SummaryRefinementPlot(luigi.Task):
    def requires(self):
        return SummaryRefinement()
    def output(self):
        return luigi.LocalTarget(Path().refinement_summary_plot)
    def run(self):
        refinement_summary_plot(refinement_csv=Path().refinement_summary,
                                out_file_path=Path().refinement_summary_plot)


class ResnameToOccLog(luigi.Task):
    def requires(self):
        return OccFromLog()
    def output(self):
        return luigi.LocalTarget(Path().log_occ_resname)
    def run(self):
        os.system("ccp4-python resnames_using_ccp4.py {} {}".format(
            Path().log_occ, Path().log_occ_resname))

class OccConvergence(luigi.Task):
    def requires(self):
        return ResnameToOccLog()
    def output(self):
        return luigi.LocalTarget(Path().occ_conv)
    def run(self):
        convergence(log_labelled_csv=Path().log_occ_resname,
                    occ_conv_csv=Path().occ_conv)

# TODO plot_occ to atomistic
class PlottingOccHistogram(luigi.Task):
    def requires(self):
        return OccConvergence()
    def output(self):
        return luigi.LocalTarget(Path().bound_occ_hist)
    def run(self):
        plot_occ()


class PrepareRefinement(luigi.Task):

    """ Links input files, and generates refinement script

    NOT WORKING
    """
    crystal = luigi.Parameter()
    pdb = luigi.Parameter()
    cif = luigi.Parameter()
    out_dir = luigi.Parameter()
    refinement_script_dir = luigi.Parameter()
    extra_params = luigi.Parameter()
    free_mtz = luigi.Parameter()

    def output(self):

        ref_script = os.path.join(self.refinement_script_dir,
                     '{}.csh'.format(self.crystal))
        return luigi.LocalTarget(ref_script)

    def requires(self):
        pass

    def run(self):
        prepare_refinement(crystal=self.crystal,
                           pdb=self.pdb,
                           cif=self.cif,
                           out_dir=self.out_dir,
                           refinement_script_dir=self.refinement_script_dir,
                           extra_params=self.extra_params,
                           free_mtz=self.free_mtz)


class QsubRefinement(luigi.Task):

    """Initiate & check progress of a single job on cluster submitted by qsub

    Notes
    ---------
    Requires the refinement script name to the name of the crystal,
    and that the pdb/mtz are stored hierarchically in that folder.
    Uses luigi.Parameter to pass a refinement script name.

    TODO Consider ways to check for existence of not just PDB

    Skeleton code adapted from working version for the formulatrix
    pipeline at diamond:

    https://github.com/xchem/formulatrix_pipe/blob/master/run_ranker.py

    TODO This job, or it's batch runner should be run multiple times to check
         that the files being created.

    TODO Check whether removal of ssh is going to cause issues
    """
    refinement_script = luigi.Parameter()

    crystal = luigi.Parameter()
    pdb = luigi.Parameter()
    cif = luigi.Parameter()
    out_dir = luigi.Parameter()
    refinement_script_dir = luigi.Parameter()
    extra_params = luigi.Parameter()
    free_mtz = luigi.Parameter()

    def requires(self):
        return PrepareRefinement(crystal=self.crystal,
                                 pdb=self.pdb,
                                 cif=self.cif,
                                 free_mtz=self.free_mtz,
                                 refinement_script_dir=Path().tmp_dir,
                                 extra_params="NCYC=50",
                                 out_dir=Path().refinement_dir)

    def output(self):
        crystal = os.path.basename(self.refinement_script.split('.')[0])
        pdb = os.path.join(Path().refinement_dir, crystal, 'refine.pdb')
        return luigi.LocalTarget(pdb)

    def run_qstat(self):

        submission_string = 'qstat -r'

        submission = subprocess.Popen(submission_string,
                                      shell=True,
                                      stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE)
        out, err = submission.communicate()

        output_queue = (out.decode('ascii').split('\n'))

        return output_queue

    def run(self):
        crystal = os.path.basename(self.refinement_script.split('.')[0])

        pdb = os.path.join(Path().refinement_dir, crystal, 'refine.pdb')
        mtz = os.path.join(Path().refinement_dir, crystal, 'refine.mtz')

        if not (os.path.isfile(pdb) and os.path.isfile(mtz)):
            queue_jobs = []
            job = self.refinement_script
            output = glob.glob(str(job + '.o*'))
            output_queue = self.run_qstat()

            for line in output_queue:
                if 'Full jobname' in line:
                    jobname = line.split()[-1]
                    queue_jobs.append(jobname)

            job_file = os.path.basename(str(job))

            print(job_file)

            if job_file not in queue_jobs:
                submit_job(job_directory=Path().tmp_dir,
                           job_script=job_file)

                print('The job had no output, and was not found to be running ' 
                      'in the queue. The job has been resubmitted. ' 
                      'Will check again later!')

            elif not queue_jobs:
                raise Exception('Something went wrong or job is still running')


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

    with open(Path().convergence_refinement_failures, 'a') as conv_ref_csv:
        conv_ref_writer = csv.writer(conv_ref_csv, delimiter=',')
        conv_ref_writer.writerow(["Failure", task.crystal, type(exception), exception])

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

    with open(Path().convergence_refinement_failures, 'a') as conv_ref_csv:
        conv_ref_writer = csv.writer(conv_ref_csv, delimiter=',')
        conv_ref_writer.writerow(["Sucesss", task.crystal])

class BatchRefinement(luigi.Task):

    """Run a Batch of refinement jobs

    This work to set the job up, and not run if already started,
    but doesn't retry to check jobs.


    Notes
    ---------

    Skeleton code adapted from working version for the formulatrix
    pipeline at diamond:

    https://github.com/xchem/formulatrix_pipe/blob/master/run_ranker.py

    (Converegence refinement) Failure modes:

    Fixed?

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

    def output(self):
        return luigi.LocalTarget(Path().convergence_refinement_failures)

    def requires(self):
        df = pd.read_csv(Path().log_pdb_mtz)

        df = df.replace(np.nan, '', regex=True)

        refinement_tasks = []

        for i in df.index:
            cif = df.at[i, 'cif']
            pdb = df.at[i, 'pdb_latest']
            mtz = df.at[i, 'mtz_free']
            crystal = df.at[i, 'crystal_name']

            # Cheat to allow run on single folder
            # if crystal != "FIH-x0439":
            #     continue

            refinement_script = os.path.join(Path().tmp_dir,
                                             "{}.csh".format(crystal))qsatt

            ref_task = QsubRefinement(
                            refinement_script=refinement_script,
                            crystal=crystal,
                            pdb=pdb,
                            cif=cif,
                            free_mtz=mtz,
                            refinement_script_dir=Path().tmp_dir,
                            extra_params="NCYC=50",
                            out_dir=Path().refinement_dir)

            refinement_tasks.append(ref_task)

        return refinement_tasks






if __name__ == '__main__':


    luigi.build([BatchRefinement()], local_scheduler=False, workers=20)

    #luigi.build([QsubRefinement(refinement_script='DCLRE1AA-x1010.csh')], local_scheduler=True)

    # luigi.build([BatchCheck()], local_scheduler=True, workers=4)

    # luigi.build([PlottingOccHistogram(),
    #              ResnameToOccLog(),
    #              SummaryRefinementPlot()],
    #             local_scheduler=True,
    #             workers=10)