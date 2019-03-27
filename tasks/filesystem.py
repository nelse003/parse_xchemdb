import os
import pandas as pd
import luigi
from luigi.util import requires
from utils.filesystem import get_most_recent_quick_refine
import tasks.batch


@requires(tasks.batch.BatchRefinement)
class RefinementFolderToCsv(luigi.Task):

    """Convert refinement folders to CSV

    Parse a refinement folder to get a csv with minimally:
        refine_log: path to refmac log
        crystal_name: crystal name
        pdb_latest: path to
        mtz_latest: path to latest mtz

    Methods
    ----------

    Attributes
    -----------

    """
    refinement_csv = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.refinement_csv)

    def run(self):

        pdb_mtz_log_dict = {}

        for crystal in os.listdir(self.out_dir):

            pdb_latest = None
            mtz_latest = None
            refinement_log = None

            crystal_dir = os.path.join(self.out_dir, crystal)

            for f in os.listdir(crystal_dir):
                if f == "refine.pdb":
                    pdb_latest = os.path.join(self.out_dir, crystal, f)
                elif f == "refine.mtz":
                    mtz_latest = os.path.join(self.out_dir, crystal, f)


            if self.refinement_type == "superposed":
                try:
                    refinement_log = get_most_recent_quick_refine(crystal_dir)
                except FileNotFoundError:
                    continue
            elif self.refinement_type == "bound":
                for f in os.listdir(crystal_dir):
                    if f == "refmac.log":
                        refinement_log = os.path.join(self.out_dir, crystal, f)


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
        df.to_csv(self.refinement_csv)