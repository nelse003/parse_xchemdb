import luigi
import os
from parse_xchemdb import process_refined_crystals
from plotting import main as plot_occ
from plotting import refinement_summary_plot

### Config ###

class Csv(luigi.Config):
    # csvs
    log_pdb_mtz = luigi.Parameter(default='log_pdb_mtz.csv')
    occ_conv = luigi.Parameter(default='occ_conv.csv')
    refinement_summary = luigi.Parameter(default='refinement_summary.csv')
    refine = luigi.Parameter(default='refinement.csv')
    superposed = luigi.Parameter(default='superposed.csv')
    occ_conv_failures = luigi.Parameter(default='occ_conv_failures.csv')

class Plot(luigi.Config):
    refinement_summary = luigi.Parameter(default='refinement_summary.png')

class Py(luigi.Config):
    convergence = luigi.Parameter(default='convergence.py')

class Path(luigi.Config, Csv, Plot, Py):
    script_dir = luigi.Parameter(default="/dls/science/groups/i04-1/elliot-dev/parse_xchemdb")
    out_dir = luigi.Parameter(default="/dls/science/groups/i04-1/elliot-dev/"
                                      "Work/exhaustive_parse_xchem_db/")

    occ_conv = luigi.Parameter(default=os.path.join(out_dir, Csv().occ_conv))

    refinement_summary_csv =  luigi.Parameter(
        default=os.path.join(out_dir, Csv().refinement_summary))
    refine_csv = luigi.Parameter(
        default=os.path.join(out_dir, Csv().refine))
    superposed_csv = luigi.Parameter(
        default=os.path.join(out_dir, Csv().superposed))
    occ_conv_failures_csv = luigi.Parameter(
        default=os.path.join(out_dir, Csv().refinement_summary))
    log_pdb_mtz_csv = luigi.Parameter(
        default=os.path.join(out_dir, Csv().log_pdb_mtz))

    refinement_summary_plot = luigi.Parameter(
        default=os.path.join(out_dir, Plot().refinement_summary))

    convergence_py = luigi.Parameter(
        default=os.path.join(script_dir, Py().convergence))


## Tasks ###

class ParseXChemDBtoDF(luigi.Task):
    def requires(self):
        return None
    def output(self):
        return luigi.LocalTarget(Path().log_pdb_mtz_csv)
    def run(self):
        process_refined_crystals()

# class CrystalTable(luigi.Task):

class SummaryRefinement(luigi.Task):
    def requires(self):
        return OccConvergence(), ParseXChemDBtoDF()
               #SuperposedDF(),
               #OccConvergenceFailureDF(),
               #RefineDF()

        # occ_conv_failures
    def output(self):
        return
    def run(self):
        refinement_summary(occ_conv_csv=Path().occ_conv,
                           refine_csv=Path().refine_csv,
                           superposed_csv=Path().superposed_csv,
                           occ_conv_failures_csv=Path().occ_conv_failures,
                           log_pdb_mtz_csv=Path().log_pdb_mtz_csv,
                           out_csv=Path().refinement_summary_plot)
        pass

class SummaryRefinementPlot(luigi.Task):
    def requires(self):
        return SummaryRefinement()
    def output(self):
        return luigi.LocalTarget(Path().refinement_summary_plot)
    def run(self):
        refinement_summary_plot()

# TODO Occupancy convergence failures to atomistic?
class OccConvergence(luigi.Task):
    def requires(self):
        return ParseXChemDBtoDF()
    def output(self):
        return luigi.LocalTarget(Path().occ_conv)
    def run(self):
        os.system("ccp4-python {}".format(Path.convergence_py))

# TODO plot_occ to atomistic
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
    luigi.build([PlottingOccHistogram(),
                 SummaryRefinementPlot()],
                local_scheduler=True,
                workers=10)