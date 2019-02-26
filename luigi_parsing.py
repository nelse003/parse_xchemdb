import luigi
import os
import pandas as pd
import glob

import cluster_submission

from parse_xchemdb import process_refined_crystals
from parse_xchemdb import get_table_df
from parse_xchemdb import drop_only_dimple_processing
from parse_xchemdb import drop_pdb_not_in_filesystem

from convergence import get_occ_from_log
from convergence import main as convergence

from plotting import main as plot_occ
from plotting import refinement_summary_plot
from refinement_summary import refinement_summary
### Config ###

class Path(luigi.Config):

    script_dir = "/dls/science/groups/i04-1/elliot-dev/parse_xchemdb"
    out_dir = "/dls/science/groups/i04-1/elliot-dev/Work/" \
              "exhaustive_parse_xchem_db/"
    tmp_dir = os.path.join(out_dir, "tmp")


    # CSVS
    log_pdb_mtz = luigi.Parameter(
        default=os.path.join(out_dir,'log_pdb_mtz.csv'))
    log_occ = luigi.Parameter(
        default=os.path.join(out_dir,'log_occ.csv'))
    log_occ_resname = luigi.Parameter(
        default=os.path.join(out_dir,'log_occ_resname.csv'))

    occ_conv = luigi.Parameter(
        default=os.path.join(out_dir,'occ_conv.csv'))
    refinement_summary = luigi.Parameter(
        default=os.path.join(out_dir,'refinement_summary.csv'))
    refine = luigi.Parameter(
        default=os.path.join(out_dir,'refinement.csv'))
    superposed = luigi.Parameter(
        default=os.path.join(out_dir,'superposed.csv'))
    occ_conv_failures = luigi.Parameter(
        default=os.path.join(out_dir,'occ_conv_failures.csv'))

    # Plots
    refinement_summary_plot = luigi.Parameter(
        default=os.path.join(out_dir,'refinement_summary.png'))
    bound_occ_hist = luigi.Parameter(
        default=os.path.join(out_dir,'bound_occ_hist.png'))

    # Scripts
    convergence_py = luigi.Parameter(
        default=os.path.join(script_dir, "convergence.py"))

    # Batch Management
    refmac_batch = luigi.Parameter(default = os.path.join(out_dir, "refmac_batch.log"))

    # Dirs
    script_dir = luigi.Parameter(default= script_dir)
    tmp_dir = luigi.Parameter(default= tmp_dir)
    out_dir = luigi.Parameter(default=out_dir)

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

class BatchCheck(luigi.Task):

    """Check whether batch jobs on cluster have been run

    NOT WORKING

    Notes
    ---------

    From https://github.com/xchem/formulatrix_pipe/blob/master/run_ranker.py
    as BatchCheckRanker

    """

    def output(self):
        return luigi.LocalTarget(Path().refmac_batch)

    def requires(self):
        # get a list of all refmac jobs
        files_list = glob.glob(os.path.join(Path().tmp_dir, 'refmac*.sh'))
        print(Path().tmp_dir)
        print(files_list)
        # Check whether the output files expected have appeared
        return [CheckQsub(name=name) for name in files_list]

    def run(self):
        with self.output().open('w') as f:
            f.write('')

class CheckQsub(luigi.Task):

    """Initiate batch jobs on cluster

    NOT WORKING

    Notes
    ---------

    From https://github.com/xchem/formulatrix_pipe/blob/master/run_ranker.py
    as CheckRanker

    """

    name = luigi.Parameter()
    data_directory = luigi.Parameter(default='Data')
    extension = luigi.Parameter(default='.mat')
    # a list of people to email when a plate has been ranked

    def requires(self):
        pass

    def output(self):
        # a text version of the email sent is saved
        return luigi.LocalTarget(os.path.join('messages', str(self.name + '.txt')))

    def run(self):
        # what we expect the output from the ranker job to be
        expected_file = os.path.join(self.data_directory, str(self.name + self.extension))
        # if it's not there, throw an error - might just not be finished... maybe change to distinguish(?)

        if not os.path.isfile(expected_file):
            time.sleep(5)
            if not os.path.isfile(expected_file):
                queue_jobs = []
                job = 'ranker_jobs/RANK_' + self.name + '.sh'
                output = glob.glob(str(job + '.o*'))
                print(output)


                remote_sub_command = 'ssh -tt jot97277@nx.diamond.ac.uk'
                submission_string = ' '.join([
                    remote_sub_command,
                    '"',
                    'qstat -r',
                    '"'
                ])

                submission = subprocess.Popen(submission_string, shell=True, stdout=subprocess.PIPE,
                                              stderr=subprocess.PIPE)
                out, err = submission.communicate()

                output_queue = (out.decode('ascii').split('\n'))
                print(output_queue)
                for line in output_queue:
                    if 'Full jobname' in line:
                        jobname = line.split()[-1]
                        queue_jobs.append(jobname)
                print(queue_jobs)
                if job.replace('ranker_jobs/', '') not in queue_jobs:
                    cluster_submission.submit_job(job_directory=os.path.join(os.getcwd(), 'ranker_jobs'),
                                                  job_script=job.replace('ranker_jobs/', ''))
                    print(
                        'The job had no output, and was not found to be running in the queue. The job has been '
                        'resubmitted. Will check again later!')
                if not queue_jobs:
                    raise Exception('.mat file not found for ' + str(
                        self.name) + '... something went wrong in ranker or job is still running')
            # if job not in queue_jobs:
            #     cluster_submission.submit_job(job_directory=os.path.join(os.getcwd(), 'ranker_jobs'),
            #                                   job_script=job.replace('ranker_jobs/', ''))
            #     print(
            #         'The job had no output, and was not found to be running in the queue. The job has been '
            #         'resubmitted. Will check again later!')



class GenererateRefmacJobs(luigi.Task):

    """ Produces Refmac Jobs

    NOT WORKING
    """

    def output(self):
        pass

    def requires(self):
        pass

    def run(self):
        pass



if __name__ == '__main__':

    # luigi.build([BatchCheck()], local_scheduler=True, workers=4)

    luigi.build([PlottingOccHistogram(),
                 ResnameToOccLog(),
                 SummaryRefinementPlot()],
                local_scheduler=True,
                workers=10)