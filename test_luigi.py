import luigi
import os

from tasks.plotting import SummaryRefinementPlot
from tasks.plotting import PlotConvergenceHistogram
from tasks.plotting import PlotOccConvScatter
from tasks.plotting import PlotGroundOccHistogram
from tasks.plotting import PlotBoundOccHistogram
from tasks.exhaustive import PlotHistExhaustiveOcc
from tasks.exhaustive import PlotScatterExhaustiveOcc

from tasks.batch import BatchRefinement
from tasks.filesystem import RefinementFolderToCsv
from tasks.update_csv import OccFromLog
from tasks.database import ParseXchemdbToCsv
from tasks.exhaustive import ExhaustiveOcc

from path_config import Path

out_dir = (
    "/dls/science/groups/i04-1/elliot-dev/Work/exhaustive_parse_xchem_db/all_10_08_19/"
)

# Paths for analysing original refinements
test_paths = Path()
test_paths.log_pdb_mtz = os.path.join(out_dir, "log_pdb_mtz.csv")
test_paths.superposed = os.path.join(out_dir, "superposed.csv")
test_paths.refine = os.path.join(out_dir, "refine.csv")
test_paths.log_occ_csv = os.path.join(out_dir, "log_occ.csv")
test_paths.log_occ_resname = os.path.join(out_dir, "log_occ_resname.csv")
test_paths.occ_state_comment_csv = os.path.join(out_dir, "occ_state_comment.csv")
test_paths.refinement_summary = os.path.join(out_dir, "refinement_summary.csv")
test_paths.occ_correct_csv = os.path.join(out_dir, "occ_correct.csv")

test_paths.refinement_summary_plot = os.path.join(out_dir, "refinement_summary.png")
test_paths.convergence_histogram = os.path.join(out_dir, "convergence_hist.png")
test_paths.occ_conv_scatter = os.path.join(out_dir, "occ_conv_scatter.png")
test_paths.ground_occ_histogram = os.path.join(out_dir, "occ_ground_histogram.png")
test_paths.bound_occ_histogram = os.path.join(out_dir, "occ_bound_histogram.png")

test_paths.tmp_dir = os.path.join(out_dir, "tmp")

# For refamc bound re-refinement

test_paths.script_dir = "/dls/science/groups/i04-1/elliot-dev/parse_xchemdb"

test_paths.convergence_refinement_failures = os.path.join(
    out_dir, "convergence_refinement_failures.csv"
)

test_paths.refinement_dir = os.path.join(out_dir, "convergence_refinement")

test_paths.convergence_refinement = os.path.join(out_dir, "convergence_refinement.csv")

test_paths.convergence_occ = os.path.join(out_dir, "convergence_refinement_occ.csv")

test_paths.convergence_occ_resname = os.path.join(
    out_dir, "convergence_refinement_occ_resname.csv"
)

test_paths.convergence_conv_hist = os.path.join(out_dir, "convergence_conv_hist.png")

test_paths.convergence_occ_conv_scatter = os.path.join(
    out_dir, "convergence_occ_conv_scatter.png"
)

test_paths.convergence_ground_hist = os.path.join(
    out_dir, "convergence_ground_occ_hist.png"
)

test_paths.convergence_bound_hist = os.path.join(
    out_dir, "convergence_bound_occ_hist.png"
)

test_paths.convergence_occ_correct = os.path.join(
    out_dir, "convergence_occ_correct.csv"
)

# Paths for bound refinement

test_paths.bound_refinement_dir = os.path.join(out_dir, "bound_refinement")
test_paths.bound_refinement_batch_csv = os.path.join(out_dir, "bound_refmac.csv")
test_paths.bound_refinement = os.path.join(out_dir, "bound_refinement_log_pdb_mtz.csv")

#test = None
# test = "FIH-x0057"
#test = 1
# test = "ACVR1A-x1242"
# test = "TBXTA-x0992"

luigi.build(
    [
        # Analyse existing refinements
        #
        # This will also call/test:
        #
        # ResnameToOccLog,
        # OccFromLog,
        # ParseXchemdbToCsv
        # RefineToCsv
        # SuperposedToCsv
        # SummaryRefinement
        # SummaryRefinementPlot(
        #     occ_state_comment_csv=test_paths.occ_state_comment_csv,
        #     log_occ_resname=test_paths.log_occ_resname,
        #     log_occ_csv=test_paths.log_occ_csv,
        #     log_pdb_mtz_csv=test_paths.log_pdb_mtz,
        #     superposed_csv=test_paths.superposed,
        #     refine_csv=test_paths.refine,
        #     refinement_summary=test_paths.refinement_summary,
        #     refinement_summary_plot=test_paths.refinement_summary_plot,
        #     script_path=test_paths.script_dir,
        #     #test=test,
        # ),
        # These will also call/test:
        #
        # ResnameToOccLog
        # OccFromLog
        # ParseXchemdbToCsv
        # StateOccupancyToCsv
        # PlotConvergenceHistogram(
        #     occ_state_comment_csv=test_paths.occ_state_comment_csv,
        #     log_occ_resname=test_paths.log_occ_resname,
        #     log_occ_csv=test_paths.log_occ_csv,
        #     log_pdb_mtz_csv=test_paths.log_pdb_mtz,
        #     occ_correct_csv=test_paths.occ_correct_csv,
        #     plot_path=test_paths.convergence_histogram,
        #     script_path=test_paths.script_dir,
        #     test=test,
        # ),
        # PlotOccConvScatter(
        #     occ_state_comment_csv=test_paths.occ_state_comment_csv,
        #     log_occ_resname=test_paths.log_occ_resname,
        #     log_occ_csv=test_paths.log_occ_csv,
        #     log_pdb_mtz_csv=test_paths.log_pdb_mtz,
        #     occ_correct_csv=test_paths.occ_correct_csv,
        #     plot_path=test_paths.occ_conv_scatter,
        #     script_path=test_paths.script_dir,
        #     test=test,
        # ),
        # PlotGroundOccHistogram(
        #     occ_state_comment_csv=test_paths.occ_state_comment_csv,
        #     log_occ_resname=test_paths.log_occ_resname,
        #     log_occ_csv=test_paths.log_occ_csv,
        #     log_pdb_mtz_csv=test_paths.log_pdb_mtz,
        #     occ_correct_csv=test_paths.occ_correct_csv,
        #     plot_path=test_paths.ground_occ_histogram,
        #     script_path=test_paths.script_dir,
        #     test=test,
        # ),
        # PlotBoundOccHistogram(
        #     occ_state_comment_csv=test_paths.occ_state_comment_csv,
        #     log_occ_resname=test_paths.log_occ_resname,
        #     log_occ_csv=test_paths.log_occ_csv,
        #     log_pdb_mtz_csv=test_paths.log_pdb_mtz,
        #     occ_correct_csv=test_paths.occ_correct_csv,
        #     plot_path=test_paths.bound_occ_histogram,
        #     script_path=test_paths.script_dir,
        #     test=test,
        # ),
    ],
    local_scheduler=False,
    workers=100,
)
# luigi.build(
#     [
#         # These will also call/test:
#         # ResnameToOccLog
#         # OccFromLog
#         # StateOccupancyToCsv
#         PlotConvergenceHistogram(
#             occ_state_comment_csv=test_paths.occ_state_comment_csv,
#             log_occ_resname=test_paths.convergence_occ_resname,
#             log_occ_csv=test_paths.convergence_occ,
#             log_pdb_mtz_csv=test_paths.convergence_refinement,
#             occ_correct_csv=test_paths.convergence_occ_correct,
#             plot_path=test_paths.convergence_conv_hist,
#             script_path=test_paths.script_dir,
#             test=test,
#         ),
#         PlotOccConvScatter(
#             occ_state_comment_csv=test_paths.occ_state_comment_csv,
#             log_occ_resname=test_paths.convergence_occ_resname,
#             log_occ_csv=test_paths.convergence_occ,
#             log_pdb_mtz_csv=test_paths.convergence_refinement,
#             occ_correct_csv=test_paths.convergence_occ_correct,
#             plot_path=test_paths.convergence_occ_conv_scatter,
#             script_path=test_paths.script_dir,
#             test=test,
#         ),
#         PlotBoundOccHistogram(
#             occ_state_comment_csv=test_paths.occ_state_comment_csv,
#             log_occ_resname=test_paths.convergence_occ_resname,
#             log_occ_csv=test_paths.convergence_occ,
#             log_pdb_mtz_csv=test_paths.convergence_refinement,
#             occ_correct_csv=test_paths.convergence_occ_correct,
#             plot_path=test_paths.convergence_bound_hist,
#             script_path=test_paths.script_dir,
#             test=test,
#         ),
#         PlotGroundOccHistogram(
#             occ_state_comment_csv=test_paths.occ_state_comment_csv,
#             log_occ_resname=test_paths.convergence_occ_resname,
#             log_occ_csv=test_paths.convergence_occ,
#             log_pdb_mtz_csv=test_paths.convergence_refinement,
#             occ_correct_csv=test_paths.convergence_occ_correct,
#             plot_path=test_paths.convergence_ground_hist,
#             script_path=test_paths.script_dir,
#             test=test,
#         ),
#     ],
#     local_scheduler=False,
#     workers=20,
# )

luigi.build(
    [
        #################################################
        # Generate new Superposed refinements REFMAC5
        # Calls BatchRefinement
        # RefinementFolderToCsv(
        #     refinement_csv=test_paths.convergence_refinement,
        #     output_csv=test_paths.convergence_refinement_failures,
        #     out_dir=test_paths.refinement_dir,
        #     tmp_dir=test_paths.tmp_dir,
        #     extra_params="NCYC 50",
        #     log_pdb_mtz_csv=test_paths.log_pdb_mtz,
        #     refinement_program="refmac",
        #     refinement_type="superposed",
        # ),

        PlotBoundOccHistogram(
            #occ_state_comment_csv=test_paths.occ_state_comment_csv,
            #log_occ_resname=test_paths.log_occ_resname,
            #log_occ_csv=test_paths.log_occ_csv,
            #log_pdb_mtz_csv=test_paths.log_pdb_mtz,
            #occ_correct_csv=test_paths.occ_correct_csv,
            plot_path=os.path.join(out_dir, "refmac_superposed_occ_bound_histogram.png"),
            script_path=test_paths.script_dir,
        )

        ###################################################
        # Generate new Superposed refinements phenix
        # Calls BatchRefinement
        # RefinementFolderToCsv(
        #     refinement_csv=os.path.join(out_dir, "phenix_superposed_log_pdb_mtz.csv"),
        #     output_csv=os.path.join(out_dir, "phenix_superposed_batch.csv"),
        #     out_dir=os.path.join(out_dir, "phenix_superposed"),
        #     extra_params="refinement.main.number_of_macro_cycles=30",
        #     tmp_dir=test_paths.tmp_dir,
        #     log_pdb_mtz_csv=os.path.join(out_dir, "log_pdb_mtz.csv"),
        #     refinement_program="phenix",
        #     refinement_type="superposed",
        # ),
        #####################################################
        # Generate new unconstrained refinements REFMAC5
        # Calls BatchRefinement
        # RefinementFolderToCsv(
        #     refinement_csv=test_paths.bound_refinement,
        #     output_csv=test_paths.bound_refinement_batch_csv,
        #     out_dir=test_paths.bound_refinement_dir,
        #     tmp_dir=test_paths.tmp_dir,
        #     log_pdb_mtz_csv=test_paths.log_pdb_mtz,
        #     ncyc=50,
        #     refinement_type="bound",
        #     refinement_program="refmac",
        # ),
        # PDK2-x0885
        # unable to find a dictionary for residue " TF3"!
        # Works for NUDT5A-x1298
        ####################################################
        # Generate new refinement buster superposed
        # RefinementFolderToCsv(
        #     refinement_csv=os.path.join(out_dir, "buster_superposed_log_pdb_mtz.csv"),
        #     output_csv=os.path.join(out_dir, "buster_superposed_batch.csv"),
        #     out_dir=os.path.join(out_dir, "buster_superposed"),
        #     extra_params=None,
        #     tmp_dir=test_paths.tmp_dir,
        #     log_pdb_mtz_csv=os.path.join(out_dir, "log_pdb_mtz.csv"),
        #     refinement_program="buster",
        #     refinement_type="superposed",
        # ),
        # Generate new unconstrained refinements phenix
        # RefinementFolderToCsv(
        #     refinement_csv=os.path.join(out_dir, "phenix_log_pdb_mtz.csv"),
        #     output_csv=os.path.join(out_dir, "phenix_batch.csv"),
        #     out_dir=os.path.join(out_dir, "phenix"),
        #     extra_params="",
        #     ncyc=30,
        #     tmp_dir=test_paths.tmp_dir,
        #     log_pdb_mtz_csv=os.path.join(out_dir, "log_pdb_mtz.csv"),
        #     refinement_program="phenix",
        #     refinement_type="bound",
        # ),
        # Generate new exhaustive search refinements
        # RefinementFolderToCsv(
        #     refinement_csv=os.path.join(out_dir, "exhaustive_log_pdb_mtz.csv"),
        #     output_csv=os.path.join(out_dir, "exhaustive_batch.csv"),
        #     out_dir=os.path.join(out_dir, "exhaustive"),
        #     extra_params="",
        #     tmp_dir=test_paths.tmp_dir,
        #     log_pdb_mtz_csv=os.path.join(out_dir, "log_pdb_mtz.csv"),
        #     refinement_program="exhaustive",
        #     refinement_type="superposed",
        # ),

        # ExhaustiveOcc(out_dir=os.path.join(out_dir, "exhaustive"),
        #               in_csv="exhaustive_search.csv",
        #               out_csv=os.path.join(out_dir, "exhaustive_minima.csv")),
        #
        # PlotScatterExhaustiveOcc(out_csv=os.path.join(out_dir, "exhaustive_minima.csv"),
        #                   out_file=os.path.join(out_dir, "exhaustive_scatter_occ_b.png")),
        #
        # PlotHistExhaustiveOcc(out_csv=os.path.join(out_dir, "exhaustive_minima.csv"),
        #                   out_file=os.path.join(out_dir, "exhaustive_hist_occ_b"))

        # Generate new unconstrained refinements buster
        # PDK2-x0885
        # unable to find a dictionary for residue " TF3"!
        # Works for NUDT5A-x1298
        # RefinementFolderToCsv(
        #     refinement_csv=os.path.join(out_dir, "buster_log_pdb_mtz.csv"),
        #     output_csv=os.path.join(out_dir, "buster_batch.csv"),
        #     out_dir=os.path.join(out_dir, "buster"),
        #     extra_params="",
        #     tmp_dir=test_paths.tmp_dir,
        #     log_pdb_mtz_csv=os.path.join(out_dir, "log_pdb_mtz.csv"),
        #     refinement_program="buster",
        #     refinement_type="bound",
        # ),
    ],
    local_scheduler=False,
    workers=100,
)
