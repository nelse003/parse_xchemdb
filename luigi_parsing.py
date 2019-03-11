import luigi
import os
import pandas as pd
import numpy as np
import glob
import subprocess
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
from refinement import write_refmac_csh
from refinement_summary import refinement_summary

# Config


class Path(luigi.Config):

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

class ParseXChemDBToDF(luigi.Task):
    def requires(self):
        return None
    def output(self):
        return luigi.LocalTarget(Path().log_pdb_mtz)
    def run(self):
        process_refined_crystals()

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
        return OccConvergence(), ParseXChemDBToDF(), SuperposedToDF(), RefineToDF()
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

class OccFromLog(luigi.Task):
    def requires(self):
        return ParseXChemDBToDF()
    def output(self):
        return luigi.LocalTarget(Path().log_occ)
    def run(self):
        get_occ_from_log(log_pdb_mtz_csv=Path().log_pdb_mtz,
                         log_occ_csv=Path().log_occ)

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
        write_refmac_csh(crystal=self.crystal,
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

    TODO This currently needs ssh sign in credentials,
         it should be able to get them from a key file
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

class BatchRefinement(luigi.Task):

    """Check whether batch jobs on cluster have been run

    This work to set the job up, and not run if already started,
    but doesn't retry to check jobs.


    Notes
    ---------

    Skeleton code adapted from working version for the formulatrix
    pipeline at diamond:

    https://github.com/xchem/formulatrix_pipe/blob/master/run_ranker.py

    """

    def output(self):
        return luigi.LocalTarget(Path().refmac_batch)

    def requires(self):
        df = pd.read_csv(Path().log_pdb_mtz)

        df = df.replace(np.nan, '', regex=True)

        refinement_tasks = []

        for i in df.index:
            cif = df.at[i, 'cif']
            pdb = df.at[i, 'pdb_latest']
            mtz = df.at[i, 'mtz_free']
            crystal = df.at[i, 'crystal_name']

            refinement_script = os.path.join(Path().tmp_dir,
                                             "{}.csh".format(crystal))

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

    def run(self):
        with self.output().open('w') as f:
            f.write("{}".format())




if __name__ == '__main__':


    luigi.build([BatchRefinement()], local_scheduler=False, workers=20)

    #luigi.build([QsubRefinement(refinement_script='DCLRE1AA-x1010.csh')], local_scheduler=True)

    # luigi.build([BatchCheck()], local_scheduler=True, workers=4)

    # luigi.build([PlottingOccHistogram(),
    #              ResnameToOccLog(),
    #              SummaryRefinementPlot()],
    #             local_scheduler=True,
    #             workers=10)