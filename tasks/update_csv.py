import os
import luigi
from luigi.util import requires
import shutil

from annotate_ligand_state import annotate_csv_with_state_comment
from annotate_ligand_state import state_occupancies
from annotate_ligand_state import convergence_state_by_refinement_type
from refinement.parse_refmac_logs import get_occ_from_log
from path_config import Path

from utils.refinement_summary import refinement_summary
from tasks.database import ParseXchemdbToCsv, SuperposedToCsv
from tasks.filesystem import RefinementFolderToCsv

# @requires(RefinementFolderToCsv)
class OccFromPdb(luigi.Task):
    """
    Append ligand occupancy and b factor to csv containing at least the path to pdb

    Methods
    -------

    Notes
    ------

    """

    log_pdb_mtz_csv = luigi.Parameter()
    occ_correct_csv = luigi.Parameter()
    script_path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.occ_correct_csv)

    def run(self):
        os.system("source {}".format(Path().ccp4))

        print(
            "ccp4-python {}/ccp4/get_occ_b_from_pdb.py {} {}".format(
                self.script_path, self.log_pdb_mtz_csv, self.occ_correct_csv
            )
        )

        os.system(
            "ccp4-python {}/ccp4/get_occ_b_from_pdb.py {} {}".format(
                self.script_path, self.log_pdb_mtz_csv, self.occ_correct_csv
            )
        )


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
    refinement_program = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.log_occ_csv)

    def run(self):
        get_occ_from_log(
            log_pdb_mtz_csv=self.log_pdb_mtz_csv, log_occ_csv=self.log_occ_csv
        )


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

    Attributes
    ----------
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

    script_dir: luigi.Parameter()
        path to script directory for use of ccp4 scripts

    Notes
    ------
    Requires ccp4-python
    """

    log_occ_resname = luigi.Parameter()
    script_path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.log_occ_resname)

    def run(self):
        os.system("source {}".format(Path().ccp4))
        os.system(
            "ccp4-python {}/ccp4/get_resname_b.py {} {}".format(
                self.script_path, self.log_occ_csv, self.log_occ_resname
            )
        )


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

    script_dir: luigi.Parameter()
        path to script directory for use of ccp4 scripts

    TODO Add a progress bar and/or parallelise task
    """

    occ_state_comment_csv = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.occ_state_comment_csv)

    def run(self):
        annotate_csv_with_state_comment(
            self.log_occ_resname, self.occ_state_comment_csv
        )


@requires(OccStateComment, SuperposedToCsv)
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

    Notes
    -----
    As requires:

        ResnameToOccLog
        OccFromLog
        ParseXchemdbToCsv
        OccStateComment
        SuperposedToCsv

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

    occ_state_comment_csv: luigi.Parameter()
        path to csv that has state ("bound" or "ground")

    superposed_csv: luigi.Parameter()
        path to csv file detailing files that have a superposed pdb file,
         output

    refine_csv: luigi.Parameter()
        path to csv file showing refinemnet table, input

    refinement_sumamry: luigi.Parameter()
        path to csv file summarising refinement

    script_dir: luigi.Parameter()
        path to script directory for use of ccp4 scripts
    """

    refinement_summary = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.refinement_summary)

    def run(self):
        refinement_summary(
            occ_state_comment_csv=self.occ_state_comment_csv,
            refine_csv=self.refine_csv,
            superposed_csv=self.superposed_csv,
            log_pdb_mtz_csv=self.log_pdb_mtz_csv,
            out_csv=self.refinement_summary,
        )


@requires(OccStateComment)
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

    Notes
    -----


    Attributes
    ----------
    Inherited

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

    script_dir: luigi.Parameter()
        path to script directory for use of ccp4 scripts

    occ_correct_csv: luigi.paramter()
        path to csv with convergence information,
        and occupancy for the "bound" or "ground" state
    """

    occ_correct_csv = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.occ_correct_csv)

    def run(self):
        state_occupancies(
            occ_state_comment_csv=self.occ_state_comment_csv,
            occ_correct_csv=self.occ_correct_csv,
        )


class ConvergenceStateByRefinementType(luigi.Task):

    occ_csv = luigi.Parameter()
    occ_conv_state_csv = luigi.Parameter()
    refinement_type = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.occ_conv_state_csv)

    def requires(self):
        RefinementFolderToCsv(
            output_csv=self.occ_csv, input_folder=self.bound_refinement_dir
        ),

    def run(self):
        convergence_state_by_refinement_type(
            occ_csv=self.occ_csv,
            occ_conv_state_csv=self.occ_conv_state_csv,
            refinement_type=self.refinement_type,
        )
