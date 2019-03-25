import luigi

from path_config import Path

from tasks import BatchSuperposedRefinement
from tasks import SuperposedRefinementFolderToCsv
from tasks import OccFromLog
from tasks import ResnameToOccLog
from tasks import OccConvergence
from tasks import StateOccupancyToCsv

from plotting_tasks import PlotConvergenceHistogram
from plotting_tasks import PlotBoundOccHistogram
from plotting_tasks import PlotGroundOccHistogram
from plotting_tasks import PlotOccConvScatter

if __name__ == '__main__':

    # This build is for the convergence refinement case,
    # TODO A parameterised version of original task towards batch refinement

    luigi.build([BatchSuperposedRefinement(),

                 SuperposedRefinementFolderToCsv(out_csv=Path().convergence_refinement,
                                       input_folder=Path().refinement_dir),

                 OccFromLog(log_pdb_mtz_csv=Path().convergence_refinement,
                            log_occ_csv=Path().convergence_occ),

                 ResnameToOccLog(log_occ_csv=Path().convergence_occ,
                                 log_occ_resname=Path().convergence_occ_resname,
                                 log_pdb_mtz_csv=Path().convergence_refinement),

                 OccConvergence(log_occ_resname=Path().convergence_occ_resname,
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


    # luigi.build([PlottingOccHistogram(),
    #              ResnameToOccLog(),
    #              SummaryRefinementPlot()],
    #             local_scheduler=True,
    #             workers=10)