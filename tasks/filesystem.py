import luigi
from luigi.util import requires

from utils.filesystem import parse_refinement_folder

import tasks.batch


@requires(tasks.batch.BatchRefinement)
class RefinementFolderToCsv(luigi.Task):

    """Convert refinement folders to CSV

    Parse a refinement folder to get a csv with minimally:
        refine_log: path to refinement log
        crystal_name: crystal name
        pdb_latest: path to
        mtz_latest: path to latest mtz

    The folder structure to be parsed is:
        ./<crystal>/refine.pdb
        ./<crystal>/refine.mtz
        ./<crystal>/refmac.log or ./<crystal>/refine_XXXX/*quick.refine.log

    Methods
    ----------


    Attributes
    -----------
    refinement_csv: luigi.Parameter
        path to csv to store refinement information

    refinement type: luigi.Parameter()
        "ground", "bound" or "superposed" to separate out different
        refinement types

    log_pdb_mtz_csv: luigi.Parameter()
        summary csv contianing at least path to pdb, mtz
        and refinement log file from original refinement/ Database.
        This needs to exist before the batch refinement,
        not be written by it

    out_dir: luigi.Parameter()
        output directory

    tmp_dir: luigi.Parameter()
        temporary directory to hold scripts

    extra_params: luigi.Parameter()
        extra parameters to provide to superposed refinement


    """

    refinement_csv = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.refinement_csv)

    def run(self):
        parse_refinement_folder(
            refinement_dir=self.out_dir,
            refinement_csv=self.refinement_csv,
            refinement_type=self.refinement_type,
        )
