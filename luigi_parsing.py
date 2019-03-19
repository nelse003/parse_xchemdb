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
from convergence import convergence_to_csv

from refinement import prepare_superposed_refinement
from refinement_summary import refinement_summary
from refinement import get_most_recent_quick_refine
from refinement import split_conformations
from refinement import prepare_refinement

from plotting import refinement_summary_plot
from plotting import ground_state_occupancy_histogram
from plotting import bound_state_occ_histogram
from plotting import occupancy_vs_convergence
from plotting import convergence_ratio_histogram

# Config
class Path(luigi.Config):

    """Config: Paths to be used """

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

    # CSV from convergence occupancy
    convergence_refinement_failures = luigi.Parameter(
        default=os.path.join(out_dir, 'convergence_refinement.csv'))

    convergence_refinement = luigi.Parameter(
        default=os.path.join(out_dir, 'convergence_refinement_log_pdb_mtz.csv'))

    convergence_occ = luigi.Parameter(
        default=os.path.join(out_dir, 'convergence_refinement_occ.csv'))

    convergence_occ_resname = luigi.Parameter(
        default=os.path.join(out_dir, 'convergence_refinement_occ_resname.csv'))

    convergence_occ_conv = luigi.Parameter(
        default=os.path.join(out_dir, 'convergence_refinement_occ_conv.csv'))

    # Plots
    refinement_summary_plot = luigi.Parameter(
        default=os.path.join(out_dir, 'refinement_summary.png'))
    bound_occ_hist = luigi.Parameter(
        default=os.path.join(out_dir, 'bound_occ_hist.png'))

    # Plots convergence
    convergence_bound_hist = luigi.Parameter(
        default=os.path.join(out_dir, 'convergence_bound_occ_hist.png'))
    convergence_ground_hist = luigi.Parameter(
        default=os.path.join(out_dir, 'convergence_ground_occ_hist.png'))
    convergence_occ_conv_scatter = luigi.Parameter(
        default=os.path.join(out_dir, 'convergence_occ_conv_scatter.png'))
    convergence_conv_hist = luigi.Parameter(
        default=os.path.join(out_dir, 'convergence_conv_hist.png'))
    convergence_occ_correct = luigi.Parameter(default=os.path.join(out_dir,"occ_correct.csv"))

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


class ParseXchemdbToCsv(luigi.Task):
    """Task to parse Postgres tables into csv

    Methods
    --------
    requires()
        No requirements for this task
    output()
        output of task is the path to the csv
        file with refinement and crystal details
        from xchemdb
    run(out_csv)
        runs parse_xchemdb.process_refined_crystals()

    """

    def requires(self):
        return None
    def output(self):
        return luigi.LocalTarget(Path().log_pdb_mtz)
    def run(self):
        process_refined_crystals(out_csv=Path().log_pdb_mtz)

class OccFromLog(luigi.Task):
    """Task to get occupancy convergence across refinement

    Methods
    --------
    requires()
        ParseXChemdbToCsv(), csv of crystal and
        refinement table for all crystal with pdb_latest
    output()
        output of task is the path to the csv
        with occupancies of residues involved in
        complete groups.
    run()
        runs convergence.get_occ_log,
        which gets the occupancy information from quick-refine log

    """
    log_occ_csv = luigi.Parameter()
    log_pdb_mtz_csv = luigi.Parameter()

    def requires(self):
        return ParseXchemdbToCsv()

    def output(self):
        return luigi.LocalTarget(self.log_occ_csv)

    def run(self):
        get_occ_from_log(log_pdb_mtz_csv=self.log_pdb_mtz_csv,
                         log_occ_csv=self.log_occ_csv)


class RefinementFolderToCsv(luigi.Task):

    """Convert refinement folders to CSV

    Parse a refinement folder to get a csv with minimally:
        refine_log: path to quick-refine log
        crystal_name: crystal name
        pdb_latest: path to
        mtz_latest: path to latest mtz

    Methods
    ----------
    """
    out_csv = luigi.Parameter()
    input_folder = luigi.Parameter()

    def requires(self):
        BatchRefinement()

    def output(self):
        return luigi.LocalTarget(self.out_csv)

    def run(self):

        pdb_mtz_log_dict = {}

        for crystal in os.listdir(self.input_folder):

            pdb_latest = None
            mtz_latest = None
            refinement_log = None

            crystal_dir = os.path.join(self.input_folder, crystal)

            for f in os.listdir(crystal_dir):
                if f == "refine.pdb":
                    pdb_latest = os.path.join(self.input_folder, crystal, f)
                elif f == "refine.mtz":
                    mtz_latest = os.path.join(self.input_folder, crystal, f)

            try:
                refinement_log = get_most_recent_quick_refine(crystal_dir)
            except FileNotFoundError:
                continue

            if None not in [pdb_latest, mtz_latest, refinement_log]:
                pdb_mtz_log_dict[crystal] = (pdb_latest,
                                             mtz_latest,
                                             refinement_log)


        df = pd.DataFrame.from_dict(data=pdb_mtz_log_dict,
                                    columns=['pdb_latest',
                                             'mtz_latest',
                                             'refine_log'],
                                    orient='index')
        df.index.name = 'crystal_name'
        df.to_csv(self.out_csv)



class RefineToDF(luigi.Task):
    """
    Task to get refinement postgres table as csv

    Methods
    --------
    requires()
        No requirements for this task
    output()
        output of task is the path to the csv
        of refinement table
    run()
        gets table from postgres database

    """
    def requires(self):
        return None


    def output(self):
        return luigi.LocalTarget(Path().refine)


    def run(self):
        refine_df = get_table_df('refinement')
        refine_df.to_csv(Path().refine)

class SuperposedToDF(luigi.Task):

    """
    Task to get refinements with valid pdb files

    Methods
    --------
    requires()
        csv of postgres refinement table
    output()
        output of task is the path to the csv
        of refinement table with only valid pdb files
    run()
        gets table from postgres database
    """

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


class ResnameToOccLog(luigi.Task):
    """
    Task to get add residue names to convergence occupancies

    Methods
    --------
    requires()
        csv of occupancy from quick-refine log files
    output()
        occupancy convergence csv with resnames
    run()
        resnames_using_ccp4 using ccp4-python

    Notes
    ------
    Requires ccp4-python
    # TODO does this require a source statement
    """
    log_pdb_mtz = luigi.Parameter()
    log_occ = luigi.Parameter()
    log_occ_resname = luigi.Parameter()

    def requires(self):
        return OccFromLog(log_pdb_mtz_csv=self.log_pdb_mtz,
                          log_occ_csv=self.log_occ)


    def output(self):
        return luigi.LocalTarget(self.log_occ_resname)


    def run(self):
        os.system("ccp4-python resnames_using_ccp4.py {} {}".format(
                    self.log_occ,
                    self.log_occ_resname))


class OccConvergence(luigi.Task):
    """
    Task to add state and comment to resname labelled convergence

    Methods
    --------
    requires()
        occupancy convergence csv with resnames
    output()
        path to occupancy convergence csv with state and comments
    run()
        convergence.convergence_to_csv()

    TODO Add a progress bar and/or parallelise task
    """
    log_pdb_mtz =  luigi.Parameter()
    log_labelled_csv = luigi.Parameter()
    occ_conv_csv = luigi.Parameter()


    def requires(self):
        return ResnameToOccLog(log_occ=self.occ_conv_csv,
                               log_occ_resname=self.log_labelled_csv,
                               log_pdb_mtz=self.log_pdb_mtz)

    def output(self):
        return luigi.LocalTarget(self.occ_conv_csv)


    def run(self):
        convergence_to_csv(self.log_labelled_csv,
                           self.occ_conv_csv)


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
        return OccConvergence(log_labelled_csv=Path().log_occ_resname,
                              occ_conv_csv=Path().occ_conv,
                              log_pdb_mtz=Path().log_pdb_mtz),\
               ParseXchemdbToCsv(), \
               SuperposedToDF(),\
               RefineToDF()

    def output(self):
        return luigi.LocalTarget(Path().refinement_summary)
    def run(self):
        refinement_summary(occ_conv_csv=Path().occ_conv,
                           refine_csv=Path().refine,
                           superposed_csv=Path().superposed,
                           log_pdb_mtz_csv=Path().log_pdb_mtz,
                           out_csv=Path().refinement_summary)

class SummaryRefinementPlot(luigi.Task):
    """
    Task to produce a plot summarising refinement

    Methods
    --------
    requires()
        csv summarising refinement
    output()
        plot file path
    run()
        plotting from csv
    """

    def requires(self):
        return SummaryRefinement()

    def output(self):
        return luigi.LocalTarget(Path().refinement_summary_plot)

    def run(self):
        refinement_summary_plot(refinement_csv=Path().refinement_summary,
                                out_file_path=Path().refinement_summary_plot)


class PlotGroundOccHistogram(luigi.Task):

    """Task to plot histogram of ground occupancies

    Methods
    --------
    requires()
        csv with convergence of occupancies
    output()
        plot file path
    run()
        plotting from csv
    """
    plot_path = luigi.Parameter()
    log_labelled_csv = luigi.Parameter()
    occ_conv_csv = luigi.Parameter()
    occ_correct_csv = luigi.Parameter()
    log_pdb_mtz = luigi.Parameter()

    def requires(self):
        StateOccupancyToCsv(log_labelled_csv=Path().convergence_occ_resname,
                            occ_conv_csv=Path().convergence_occ_conv,
                            log_pdb_mtz=Path().convergence_refinement,
                            occ_correct_csv=Path().convergence_occ_correct)

    def output(self):
        return luigi.LocalTarget(self.plot_path)

    def run(self):
        ground_state_occupancy_histogram(occ_correct_csv=self.occ_correct_csv,
                                         plot_path=self.plot_path)


class PlotBoundOccHistogram(luigi.Task):
    """Task to plot histogram of bound occupancies

    Methods
    --------
    requires()
        csv with convergence of occupancies
    output()
        plot file path
    run()
        plotting from csv
    """
    plot_path = luigi.Parameter()
    log_labelled_csv = luigi.Parameter()
    occ_conv_csv = luigi.Parameter()
    occ_correct_csv = luigi.Parameter()
    log_pdb_mtz = luigi.Parameter()

    def requires(self):
        StateOccupancyToCsv(log_labelled_csv=Path().convergence_occ_resname,
                            occ_conv_csv=Path().convergence_occ_conv,
                            log_pdb_mtz=Path().convergence_refinement,
                            occ_correct_csv=Path().convergence_occ_correct)

    def output(self):
        return luigi.LocalTarget(self.plot_path)

    def run(self):
        bound_state_occ_histogram(occ_correct_csv=self.occ_correct_csv ,
                                  plot_path=self.plot_path)


class PlotOccConvScatter(luigi.Task):
    """Task to plot scatter of occupancy vs convergence

    Methods
    --------
    requires()
        csv with convergence of occupancies
    output()
        plot file path
    run()
        plotting from csv
    """
    plot_path = luigi.Parameter()
    log_labelled_csv = luigi.Parameter()
    occ_conv_csv = luigi.Parameter()
    occ_correct_csv = luigi.Parameter()
    log_pdb_mtz = luigi.Parameter()

    def requires(self):
        StateOccupancyToCsv(log_labelled_csv=Path().convergence_occ_resname,
                            occ_conv_csv=Path().convergence_occ_conv,
                            log_pdb_mtz=Path().convergence_refinement,
                            occ_correct_csv=Path().convergence_occ_correct)

    def output(self):
        return luigi.LocalTarget(self.plot_path)

    def run(self):
        occupancy_vs_convergence(occ_correct_csv=self.occ_correct_csv,
                                         plot_path=self.plot_path)


class PlotConvergenceHistogram(luigi.Task):
    """Task to plot histogram of convergence ratios

    Methods
    --------
    requires()
        csv with convergence of occupancies
    output()
        plot file path
    run()
        plotting from csv
    """
    plot_path = luigi.Parameter()
    log_labelled_csv = luigi.Parameter()
    occ_conv_csv = luigi.Parameter()
    occ_correct_csv = luigi.Parameter()
    log_pdb_mtz = luigi.Parameter()

    def requires(self):
        StateOccupancyToCsv(log_labelled_csv=Path().convergence_occ_resname,
                            occ_conv_csv=Path().convergence_occ_conv,
                            log_pdb_mtz=Path().convergence_refinement,
                            occ_correct_csv=Path().convergence_occ_correct)

    def output(self):
        return luigi.LocalTarget(self.plot_path)

    def run(self):
        convergence_ratio_histogram(occ_correct_csv=self.occ_correct_csv,
                                         plot_path=self.plot_path)


class SplitConformations(luigi.Task):
    """
    Task to run giant.split conformations

    Attributes
    -----------
    input_pdb: luigi.Parameter()
        path to sup[eposed db to be used as input
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

    def requires(self):
        return ParseXchemdbToCsv()

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
    type= luigi.Parameter()

    def requires(self):
        working_dir = os.path.join(self.out_dir, self.crystal)

        return SplitConformations(pdb=self.pdb, working_dir=working_dir)

    def output(self):


        ref_script = os.path.join(self.refinement_script_dir,
                     '{}_{}.csh'.format(self.crystal, self.type))

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

class QsubRefinement(luigi.Task):

    """Initiate & check progress of a single job on cluster submitted by qsub


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
        path to refinement script dir with '<crystal_name>_<type>.csh' file

    free_mtz: luigi.Parameter
        path to free mtz file

    refinement_script: luigi.Parameter
        path to luigi parameter

    Methods
    ---------
    requires()
        Task requires the associated PrepareRefienement Task
        with the same parameters

    output()
        Task should output refined pdb and mtz files

    run_qstat()
        wrapper for running qstat -r

    run()
        Check for presence of PDB and MTZ,
        if they do not exist, check for running jobs on cluster.
        If no running jobs, submit <crystal_name>.csh as job.

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

    TODO Check whether removal of ssh from submit job is going to cause issues

    TODO This should share an abstract class with QsubSuperposedRefinement
    """

    refinement_script = luigi.Parameter()
    crystal = luigi.Parameter()
    pdb = luigi.Parameter()
    cif = luigi.Parameter()
    out_dir = luigi.Parameter()
    refinement_script_dir = luigi.Parameter()
    free_mtz = luigi.Parameter()
    type = luigi.Parameter()

    def requires(self):
        return PrepareRefinement(crystal=self.crystal,
                                 pdb=self.pdb,
                                 cif=self.cif,
                                 out_dir=self.out_dir,
                                 refinement_script_dir=self.refinement_script_dir,
                                 free_mtz=self.free_mtz,
                                 type=self.type)

    def output(self):

        out_mtz = os.path.join(self.out_dir, "refine_{}.mtz".format(self.type))
        out_pdb = os.path.join(self.out_dir, "refine_{}.pdb".format(self.type))

        return luigi.LocalTarget(out_pdb), luigi.LocalTarget(out_mtz)

    def run_qstat(self):
        """
        Qstat wrapped to return output queue
        Returns
        -------

        """

        submission_string = 'qstat -r'

        submission = subprocess.Popen(submission_string,
                                      shell=True,
                                      stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE)
        out, err = submission.communicate()

        output_queue = (out.decode('ascii').split('\n'))

        return output_queue

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

        out_mtz = os.path.join(self.out_dir, "refine_{}.mtz".format(self.type))
        out_pdb = os.path.join(self.out_dir, "refine_{}.pdb".format(self.type))

        # Only run if the pdb and mtz are not present
        if not (os.path.isfile(out_pdb) and os.path.isfile(out_mtz)):
            queue_jobs = []

            # run 'qstat -r'
            output_queue = self.run_qstat()

            # Turn qstat output into list of jobs
            for line in output_queue:
                if 'Full jobname' in line:
                    jobname = line.split()[-1]
                    queue_jobs.append(jobname)

            # Get <crystal_name>.csh
            job = self.refinement_script
            job_file = os.path.basename(str(job))

            # Check whether <crystal_name>.csh is running in queue,
            # If not submit job to queue
            if job_file not in queue_jobs:
                submit_job(job_directory=Path().tmp_dir,
                           job_script=job_file)

                print('The job had no output, and was not found to be running ' 
                      'in the queue. The job has been resubmitted. ' 
                      'Will check again later!')

            elif not queue_jobs:
                raise RuntimeError('Something went wrong or job is still running')




class QsubSuperposedRefinement(luigi.Task):

    """Initiate & check progress of a single job on cluster submitted by qsub



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

    refinement_script: luigi.Parameter
        path to luigi parameter

    Methods
    ---------
    requires()
        Task requires the associated PrepareRefienement Task
        with the same parameters

    output()
        Task should output refined pdb and mtz files

    run_qstat()
        wrapper for running qstat -r

    run()
        Check for presence of PDB and MTZ,
        if they do not exist, check for running jobs on cluster.
        If no running jobs, submit <crystal_name>.csh as job.

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
        return PrepareSuperposedRefinement(crystal=self.crystal,
                                           pdb=self.pdb,
                                           cif=self.cif,
                                           free_mtz=self.free_mtz,
                                           refinement_script_dir=Path().tmp_dir,
                                           extra_params="NCYC=50",
                                           out_dir=Path().refinement_dir)

    def output(self):
        crystal = os.path.basename(self.refinement_script.split('.')[0])
        pdb = os.path.join(Path().refinement_dir, crystal, 'refine.pdb')
        mtz = os.path.join(Path().refinement_dir, crystal, 'refine.mtz')
        return luigi.LocalTarget(pdb), luigi.LocalTarget(mtz)

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

        # get crystal name
        crystal = os.path.basename(self.refinement_script.split('.')[0])

        # output files
        pdb = os.path.join(Path().refinement_dir, crystal, 'refine.pdb')
        mtz = os.path.join(Path().refinement_dir, crystal, 'refine.mtz')

        # Only run if the pdb and mtz are not present
        if not (os.path.isfile(pdb) and os.path.isfile(mtz)):
            queue_jobs = []

            # run 'qstat -r'
            output_queue = self.run_qstat()

            # Turn qstat output into list of jobs
            for line in output_queue:
                if 'Full jobname' in line:
                    jobname = line.split()[-1]
                    queue_jobs.append(jobname)

            # Get <crystal_name>.csh
            job = self.refinement_script
            job_file = os.path.basename(str(job))

            # Check whether <crystal_name>.csh is running in queue,
            # If not submit job to queue
            if job_file not in queue_jobs:
                submit_job(job_directory=Path().tmp_dir,
                           job_script=job_file)

                print('The job had no output, and was not found to be running ' 
                      'in the queue. The job has been resubmitted. ' 
                      'Will check again later!')

            elif not queue_jobs:
                raise RuntimeError('Something went wrong or job is still running')


@PrepareSuperposedRefinement.event_handler(luigi.Event.FAILURE)
@QsubSuperposedRefinement.event_handler(luigi.Event.FAILURE)
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

@PrepareSuperposedRefinement.event_handler(luigi.Event.SUCCESS)
@QsubSuperposedRefinement.event_handler(luigi.Event.SUCCESS)
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

    This works to set the job up, and not run if already started,
    but doesn't retry to check jobs.

    Methods
    -------
    requires()
        batch of QSubRefienment Tasks
    output()
        csv path to a csv summarising

    Notes
    ---------
    Output is only localc to the current run,
    does not include complete jobs

    Skeleton code adapted from working version for the formulatrix
    pipeline at diamond:

    https://github.com/xchem/formulatrix_pipe/blob/master/run_ranker.py

    (Converegence refinement) Failure modes:

    Issues that have been fixed

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
        """
        Batch of QsubRefinement tasks

        Returns
        -------
        refinement_tasks: list of Luigi.Tasks
            list of QSubRefinement tasks
        """
        # Read crystal/refinement table csv
        df = pd.read_csv(Path().log_pdb_mtz)

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

            refinement_script = os.path.join(Path().tmp_dir,
                                             "{}.csh".format(crystal))
            # produce refinement task
            ref_task = QsubSuperposedRefinement(
                            refinement_script=refinement_script,
                            crystal=crystal,
                            pdb=pdb,
                            cif=cif,
                            free_mtz=mtz,
                            refinement_script_dir=Path().tmp_dir,
                            extra_params="NCYC=50",
                            out_dir=Path().refinement_dir)

            # add to list of refienement tasks
            refinement_tasks.append(ref_task)

        return refinement_tasks

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
    log_labelled_csv = luigi.Parameter()
    log_pdb_mtz = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.occ_correct_csv)

    def requires(self):
        OccConvergence(log_labelled_csv=self.log_labelled_csv,
                       occ_conv_csv=self.occ_conv_csv,
                       log_pdb_mtz=self.log_pdb_mtz)

    def run(self):
        state_occupancies(occ_conv_csv=self.occ_conv_csv,
                          occ_correct_csv=self.occ_correct_csv)


if __name__ == '__main__':

    # This build is for the convergence refinement case,
    # TODO A parameterised version of original task towards batch refinement

    # luigi.build([BatchRefinement(),
    #
    #              RefinementFolderToCsv(out_csv=Path().convergence_refinement,
    #                                    input_folder=Path().refinement_dir),
    #
    #              OccFromLog(log_pdb_mtz_csv=Path().convergence_refinement,
    #                         log_occ_csv=Path().convergence_occ),
    #
    #              ResnameToOccLog(log_occ=Path().convergence_occ,
    #                              log_occ_resname=Path().convergence_occ_resname,
    #                              log_pdb_mtz=Path().convergence_refinement),
    #
    #              OccConvergence(log_labelled_csv=Path().convergence_occ_resname,
    #                             occ_conv_csv=Path().convergence_occ_conv,
    #                             log_pdb_mtz=Path().convergence_refinement),
    #
    #              StateOccupancyToCsv(log_labelled_csv=Path().convergence_occ_resname,
    #                                  occ_conv_csv=Path().convergence_occ_conv,
    #                                  log_pdb_mtz=Path().convergence_refinement,
    #                                  occ_correct_csv=Path().convergence_occ_correct),
    #
    #              PlotBoundOccHistogram(log_labelled_csv=Path().convergence_occ_resname,
    #                                    occ_conv_csv=Path().convergence_occ_conv,
    #                                    occ_correct_csv=Path().convergence_occ_correct,
    #                                    log_pdb_mtz=Path().convergence_refinement,
    #                                    plot_path=Path().convergence_bound_hist),
    #
    #              PlotGroundOccHistogram(log_labelled_csv=Path().convergence_occ_resname,
    #                                     occ_conv_csv=Path().convergence_occ_conv,
    #                                     occ_correct_csv=Path().convergence_occ_correct,
    #                                     log_pdb_mtz=Path().convergence_refinement,
    #                                     plot_path=Path().convergence_ground_hist),
    #
    #              PlotOccConvScatter(log_labelled_csv=Path().convergence_occ_resname,
    #                                 occ_conv_csv=Path().convergence_occ_conv,
    #                                 occ_correct_csv=Path().convergence_occ_correct,
    #                                 log_pdb_mtz=Path().convergence_refinement,
    #                                 plot_path=Path().convergence_occ_conv_scatter),
    #
    #              PlotConvergenceHistogram(log_labelled_csv=Path().convergence_occ_resname,
    #                                       occ_conv_csv=Path().convergence_occ_conv,
    #                                       occ_correct_csv=Path().convergence_occ_correct,
    #                                       log_pdb_mtz=Path().convergence_refinement,
    #                                       plot_path=Path().convergence_conv_hist)
    #
    #              ],
    #             local_scheduler=False, workers=20)

    # This is a builf dfor the single bound and ground refinments in refmac

    luigi.build([PrepareRefinement(crystal = "SERC-x0124",
                     pdb="/dls/science/groups/i04-1/elliot-dev/Work/exhaustive_parse_xchem_db/" \
                                   "convergence_refinement/SERC-x0124/input.pdb",
                      cif="/dls/science/groups/i04-1/elliot-dev/Work/exhaustive_parse_xchem_db/" \
                            "convergence_refinement/SERC-x0124/input.cif",
                      out_dir="/dls/science/groups/i04-1/elliot-dev/Work/exhaustive_parse_xchem_db/"\
                              "ground_refinement",
                      refinement_script_dir = "/dls/science/groups/i04-1/"\
                                            "elliot-dev/Work/exhaustive_parse_xchem_db/tmp",
                      free_mtz = "/dls/science/groups/i04-1/elliot-dev/Work/exhaustive_parse_xchem_db/" \
                                 "convergence_refinement/SERC-x0124/input.mtz",
                      type="ground"),

                QsubRefinement(refinement_script="SERC-x0124_bound.csh",
                               crystal="SERC-x0124",
                               pdb="/dls/science/groups/i04-1/elliot-dev/Work/exhaustive_parse_xchem_db/" \
                                   "convergence_refinement/SERC-x0124/input.pdb",
                               cif="/dls/science/groups/i04-1/elliot-dev/Work/exhaustive_parse_xchem_db/" \
                                   "convergence_refinement/SERC-x0124/input.cif",
                               out_dir = "/dls/science/groups/i04-1/elliot-dev/Work/exhaustive_parse_xchem_db/" \
                                             "ground_refinement",
                               refinement_script_dir = "/dls/science/groups/i04-1/" \
                                   "elliot-dev/Work/exhaustive_parse_xchem_db/tmp",
                               free_mtz = "/dls/science/groups/i04-1/elliot-dev/Work/exhaustive_parse_xchem_db/" \
                               "convergence_refinement/SERC-x0124/input.mtz",
                               type="ground")
                ],
                local_scheduler=False, workers=20)

    # luigi.build([PlottingOccHistogram(),
    #              ResnameToOccLog(),
    #              SummaryRefinementPlot()],
    #             local_scheduler=True,
    #             workers=10)