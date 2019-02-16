import luigi
import os
from parse_xchemdb import process_refined_crystals
from plotting import main as plot_occ

class XChemDB(luigi.Config):
    out_dir = luigi.Parameter(default="/dls/science/groups/i04-1/elliot-dev/"
                                      "Work/exhaustive_parse_xchem_db/")
    log_pdb_mtz_csv = luigi.Parameter(default='log_pdb_mtz.csv')

class ParseXChemDBtoDF(luigi.Task):
    def requires(self):
        return None
    def output(self):
        return luigi.LocalTarget(os.path.join(XChemDB().out_dir,XChemDB().log_pdb_mtz_csv))
    def run(self):
        process_refined_crystals()

# class CrystalTable(luigi.Task):


class OccConvergence(luigi.Task):
    def requires(self):
        return ParseXChemDBtoDF()
    def output(self):
        return luigi.LocalTarget("/dls/science/groups/i04-1/elliot-dev/"
                                 "Work/exhaustive_parse_xchem_db/"
                                 "occ_conv.csv")
    def run(self):
        os.system("ccp4-python /dls/science/groups/i04-1/elliot-dev/parse_xchemdb/convergence.py")

class PlottingOccHistogram(luigi.Task):
    def requires(self):
        return OccConvergence()
    def output(self):
        return luigi.LocalTarget("/dls/science/groups/i04-1/elliot-dev/"
                                 "Work/exhaustive_parse_xchem_db/"
                                 "bound_occ_hist.png")
    def run(self):
        plot_occ()



if __name__ == '__main__':
    luigi.build([PlottingOccHistogram(),OccConvergence()], local_scheduler=True, workers=10)