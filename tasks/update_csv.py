import os
import luigi
from luigi.util import requires

from annotate_ligand_state import annotate_csv_with_state_comment
from parse_refmac_logs import get_occ_from_log
from path_config import Path
from tasks.database import ParseXchemdbToCsv


@requires(ParseXchemdbToCsv)
class OccFromLog(luigi.Task):
    """Task to get occupancy convergence across refinement

    # TODO This requriement is incorrect/ indirect:
    #      it can depend on RefinementFolderToCsv instead

    Methods
    --------
    output()
        output of task is the path to the csv
        with occupancies of residues involved in
        complete groups.
    run()
        runs convergence.get_occ_log,
        which gets the occupancy information from quick-refine log

    Notes
    -----
    Requires via decorator:

    ParseXChemdbToCsv(), csv of crystal and
    refinement table for all crystal with pdb_latest

    """
    log_occ_csv = luigi.Parameter()
    log_pdb_mtz_csv = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.log_occ_csv)

    def run(self):
        get_occ_from_log(log_pdb_mtz_csv=self.log_pdb_mtz_csv,
                         log_occ_csv=self.log_occ_csv)


@requires(OccFromLog)
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
    # TODO Add a source statement
    """
    log_occ_resname = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.log_occ_resname)


    def run(self):
        os.system("source {}".format(Path().ccp4))
        os.system("ccp4-python resnames_using_ccp4.py {} {}".format(
                    self.log_occ_csv,
                    self.log_occ_resname))


@requires(ResnameToOccLog)
class OccStateComment(luigi.Task):
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

    Notes
    ------
    As requires:

        ResnameToOccLog
        OccFromLog
        ParseXchemdbToCsv

    attributes are extended to include those from required tasks

    Attributes
    -----------
    occ_state_comment_csv: luigi.Parameter()
         path to csv with state and comment

    log_occ_resname: luigi.Parameter()
         path to csv annotate with residue names for
         residues involved in complete refinment groups,
         as parsed from the REFMAC5 Log file

    log_occ_csv: luigi.Parameter()
        path to csv file where occupancies have
        been obtained from REFMAC logs

    log_pdb_mtz_csv: luigi.Parameter()
        path to summary csv containing at least path to pdb, mtz
        and refinement log file

    test: luigi.Parameter(), optional
        integer representing number of rows to extract when
        used as test

    TODO Add a progress bar and/or parallelise task
    """

    occ_state_comment_csv = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.occ_state_comment_csv)


    def run(self):
        annotate_csv_with_state_comment(self.log_occ_resname,
                                        self.occ_state_comment_csv)