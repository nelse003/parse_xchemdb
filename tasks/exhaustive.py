import luigi
from utils.exhaustive import find_all_exhaustive_minima
from utils.exhaustive import plot_scatter_exhaustive_minima
from utils.exhaustive import plot_hist_exhaustive_minima

class ExhaustiveOcc(luigi.Task):

    """
    out_dir: luigi.Parameter()
        Directory where output of exhaustive search
    in_csv: luigi.Parameter()
        filename of input_csv
    out_csv: luigi.Parameter()
        output csv file containing minima
    """

    out_dir = luigi.Parameter()
    in_csv = luigi.Parameter()
    out_csv = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.out_csv)

    def run(self):
        find_all_exhaustive_minima(out_dir=self.out_dir,
                               in_csv=self.in_csv,
                               out_csv=self.out_csv)

class PlotScatterExhaustiveOcc(luigi.Task):

    """
    out_csv: luigi.Parameter()
        output csv file containing minima
    """
    out_csv = luigi.Parameter()
    out_file = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.out_file)

    def run(self):
        plot_scatter_exhaustive_minima(out_csv=self.out_csv,
                                   out_file=self.out_file)

class PlotHistExhaustiveOcc(luigi.Task):

    """
    out_csv: luigi.Parameter()
        output csv file containing minima
    """
    out_csv = luigi.Parameter()
    out_file = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.out_file)

    def run(self):
        plot_hist_exhaustive_minima(out_csv=self.out_csv,
                                   out_file=self.out_file)