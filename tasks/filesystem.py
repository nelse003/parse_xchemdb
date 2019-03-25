from path_config import Path
from refinement import get_most_recent_quick_refine
from tasks import BatchRefinement


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
    output_csv = luigi.Parameter()
    input_folder = luigi.Parameter()
    refinement_type = luigi.Parameter()

    def requires(self):
        return BatchRefinement(out_dir=Path().bound_refinement_dir,
                               output_csv=Path().bound_refinement_batch_csv,
                               refinement_type=self.refinement_type)

    def output(self):
        return luigi.LocalTarget(self.output_csv)

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


            if self.refinement_type == "superposed":
                try:
                    refinement_log = get_most_recent_quick_refine(crystal_dir)
                except FileNotFoundError:
                    continue
            elif self.refinement_type == "bound":
                for f in os.listdir(crystal_dir):
                    if f == "refmac.log":
                        refinement_log = os.path.join(self.input_folder, crystal, f)


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
        df.to_csv(self.output_csv)