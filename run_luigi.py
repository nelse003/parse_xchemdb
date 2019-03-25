import luigi

from path_config import Path

from tasks import SuperposedRefinementFolderToCsv
from tasks import StateOccupancyToCsv
from tasks import BatchRefinement
from tasks.update_csv import OccFromLog, ResnameToOccLog, OccStateComment
from tasks import ConvergenceStateByRefinementType
from tasks.filesystem import RefinementFolderToCsv

from tasks.plotting import PlotConvergenceHistogram
from tasks.plotting import PlotOccConvScatter
from tasks.plotting import SummaryRefinementPlot
from tasks.plotting import PlotGroundOccHistogram
from tasks.plotting import PlotBoundOccHistogram

if __name__ == '__main__':

    # This build is for the convergence refinement case,

    luigi.build([BatchRefinement(output_csv=Path().convergence_refinement_failures),
                                 refinement_type="superposed",
                                 out_dir=Path().refinement_dir),

    SuperposedRefinementFolderToCsv(out_csv=Path().convergence_refinement,
                                       input_folder=Path().refinement_dir),

    OccFromLog(log_pdb_mtz_csv=Path().convergence_refinement,
                            log_occ_csv=Path().convergence_occ),

    ResnameToOccLog(log_occ_csv=Path().convergence_occ,
                                 log_occ_resname=Path().convergence_occ_resname,
                                 log_pdb_mtz_csv=Path().convergence_refinement),

    OccStateComment(log_occ_resname=Path().convergence_occ_resname,
                    occ_conv_csv=Path().convergence_occ_conv,
                    log_pdb_mtz_csv=Path().convergence_refinement,
                    log_occ_csv=Path().convergence_occ),

    StateOccupancyToCsv(log_occ_resname=Path().convergence_occ_resname,
                                     occ_conv_csv=Path().convergence_occ_conv,
                                     log_pdb_mtz=Path().convergence_refinement,
                                     occ_correct_csv=Path().convergence_occ_correct),

    PlotBoundOccHistogram(log_occ_resname=Path().convergence_occ_resname,
                                       occ_conv_csv=Path().convergence_occ_conv,
                                       occ_correct_csv=Path().convergence_occ_correct,
                                       log_pdb_mtz=Path().convergence_refinement,
                                       plot_path=Path().convergence_bound_hist),

    PlotGroundOccHistogram(log_occ_resname=Path().convergence_occ_resname,
                                        occ_conv_csv=Path().convergence_occ_conv,
                                        occ_correct_csv=Path().convergence_occ_correct,
                                        log_pdb_mtz=Path().convergence_refinement,
                                        plot_path=Path().convergence_ground_hist),

    PlotOccConvScatter(log_occ_resname=Path().convergence_occ_resname,
                                    occ_conv_csv=Path().convergence_occ_conv,
                                    occ_correct_csv=Path().convergence_occ_correct,
                                    log_pdb_mtz=Path().convergence_refinement,
                                    plot_path=Path().convergence_occ_conv_scatter),

    PlotConvergenceHistogram(log_occ_resname=Path().convergence_occ_resname,
                                          occ_conv_csv=Path().convergence_occ_conv,
                                          occ_correct_csv=Path().convergence_occ_correct,
                                          log_pdb_mtz=Path().convergence_refinement,
                                          plot_path=Path().convergence_conv_hist)
                 ],
                local_scheduler=False, workers=20)


    # For REFMAC5 bound state refinement

    luigi.build([BatchRefinement(
        out_dir=Path().bound_refinement_dir,
        output_csv=Path().bound_refinement_batch_csv,
        refinement_type="bound"),

        # Needed refactoring
        RefinementFolderToCsv(output_csv=Path().bound_refinement,
                              input_folder=Path().bound_refinement_dir),

        OccFromLog(log_pdb_mtz_csv=Path().bound_refinement,
                   log_occ_csv=Path().bound_occ),

        #Needed refactoring into new task
        ConvergenceStateByRefinementType(occ_csv=Path().bound_occ,
                                         occ_conv_state_csv=Path().bound_occ_conv_state,
                                         refinement_type="bound")

    ],
    local_scheduler=False, workers=20)


