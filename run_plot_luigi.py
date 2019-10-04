import luigi
import os
from tasks.plotting import PlotEdstatMetric
from tasks.plotting import PlotEdstatsDistPlot
from tasks.plotting import PlotEdstats
from tasks.plotting import PlotOccKde
from tasks.plotting import PlotOccDiffKde
from tasks.plotting import PlotBKde
from tasks.plotting import PlotBViolin
from tasks.plotting import PlotOccViolin

from tasks.filesystem import SummariseEdstats
from tasks.filesystem import EdstatsResultSummaries

from tasks.exhaustive import ExhaustiveOcc
from tasks.exhaustive import PlotHistExhaustiveOcc

out_dir = (
    "/dls/science/groups/i04-1/elliot-dev/Work/"
    "exhaustive_parse_xchem_db/all_10_08_19/"
)

luigi.build(
    [
        # ExhaustiveOcc(out_dir=os.path.join(out_dir,"exhaustive"),
        #               in_csv="exhaustive_search.csv",
        #               out_csv="exhaustive_minima.csv")

        # PlotHistExhaustiveOcc(out_csv=os.path.join(out_dir,
        #                                            "exhaustive_minima.csv"),
        #                       out_file=os.path.join(out_dir,
        #                                             "exhaustive_histogram.png"))

        # PlotOccKde(
        #     refmac_occ_correct_csv = os.path.join(out_dir, "refmac_occ_correct.csv"),
        #     refmac_superposed_occ_correct_csv = os.path.join(out_dir, "refmac_superposed_occ_correct.csv"),
        #     phenix_superposed_occ_correct_csv = os.path.join(out_dir, "phenix_superposed_occ_correct.csv"),
        #     phenix_occ_correct_csv = os.path.join(out_dir, "phenix_occ_correct.csv"),
        #     buster_superposed_occ_correct_csv=os.path.join(out_dir, "buster_superposed_occ_correct.csv"),
        #     buster_occ_correct_csv=os.path.join(out_dir, "buster_occ_correct.csv"),
        #     exhaustive_csv = os.path.join(out_dir, "exhaustive_minima.csv"),
        #     plot_path = os.path.join(out_dir, "occ_kde.png")
        # ),
        # PlotOccDiffKde(
        #     refmac_occ_correct_csv = os.path.join(out_dir, "refmac_occ_correct.csv"),
        #     refmac_superposed_occ_correct_csv = os.path.join(out_dir, "refmac_superposed_occ_correct.csv"),
        #     phenix_superposed_occ_correct_csv = os.path.join(out_dir, "phenix_superposed_occ_correct.csv"),
        #     phenix_occ_correct_csv = os.path.join(out_dir, "phenix_occ_correct.csv"),
        #     buster_superposed_occ_correct_csv=os.path.join(out_dir, "buster_superposed_occ_correct.csv"),
        #     buster_occ_correct_csv=os.path.join(out_dir, "buster_occ_correct.csv"),
        #     exhaustive_csv = os.path.join(out_dir, "exhaustive_minima.csv"),
        #     plot_path = os.path.join(out_dir, "occ_diff_kde.png")
        # ),
        #
        # PlotBKde(
        #     refmac_occ_correct_csv = os.path.join(out_dir, "refmac_occ_correct.csv"),
        #     refmac_superposed_occ_correct_csv = os.path.join(out_dir, "refmac_superposed_occ_correct.csv"),
        #     phenix_superposed_occ_correct_csv = os.path.join(out_dir, "phenix_superposed_occ_correct.csv"),
        #     phenix_occ_correct_csv = os.path.join(out_dir, "phenix_occ_correct.csv"),
        #     buster_superposed_occ_correct_csv=os.path.join(out_dir, "buster_superposed_occ_correct.csv"),
        #     buster_occ_correct_csv=os.path.join(out_dir, "buster_occ_correct.csv"),
        #     exhaustive_csv = os.path.join(out_dir, "exhaustive_minima.csv"),
        #     plot_path = os.path.join(out_dir, "B_kde.png")
        # ),
        # PlotBViolin(
        #     refmac_occ_correct_csv = os.path.join(out_dir, "refmac_occ_correct.csv"),
        #     refmac_superposed_occ_correct_csv = os.path.join(out_dir, "refmac_superposed_occ_correct.csv"),
        #     phenix_superposed_occ_correct_csv = os.path.join(out_dir, "phenix_superposed_occ_correct.csv"),
        #     phenix_occ_correct_csv = os.path.join(out_dir, "phenix_occ_correct.csv"),
        #     buster_superposed_occ_correct_csv=os.path.join(out_dir, "buster_superposed_occ_correct.csv"),
        #     buster_occ_correct_csv=os.path.join(out_dir, "buster_occ_correct.csv"),
        #     exhaustive_csv = os.path.join(out_dir, "exhaustive_minima.csv"),
        #     plot_path = os.path.join(out_dir, "B_violin.png")
        # ),
        # PlotOccViolin(
        #     refmac_occ_correct_csv=os.path.join(out_dir, "refmac_occ_correct.csv"),
        #     refmac_superposed_occ_correct_csv=os.path.join(out_dir, "refmac_superposed_occ_correct.csv"),
        #     phenix_superposed_occ_correct_csv=os.path.join(out_dir, "phenix_superposed_occ_correct.csv"),
        #     phenix_occ_correct_csv=os.path.join(out_dir, "phenix_occ_correct.csv"),
        #     buster_superposed_occ_correct_csv=os.path.join(out_dir, "buster_superposed_occ_correct.csv"),
        #     buster_occ_correct_csv=os.path.join(out_dir, "buster_occ_correct.csv"),
        #     exhaustive_csv=os.path.join(out_dir, "exhaustive_minima.csv"),
        #     plot_path=os.path.join(out_dir, "Occ_violin.png")
        #  ),

        #EdstatsResultSummaries(out_dir=out_dir),

        # PlotEdstats(out_dir=out_dir),
        PlotEdstatsDistPlot(metric='RSCC',
                            out_dir=out_dir,
                            plot_path=os.path.join(out_dir,
                                                   "edstats_figures",
                                                   "RSCC_distplot.png")),
        # PlotEdstatsDistPlot(metric='RSZO',
        #                     out_dir=out_dir,
        #                     plot_path=os.path.join(out_dir,
        #                                            "edstats_figures",
        #                                            "RSZO_distplot.png"),
        #                     xlim=(-0.5, 5)),
        PlotEdstatsDistPlot(metric='RSZO/OCC',
                            out_dir=out_dir,
                            plot_path=os.path.join(out_dir,
                                                   "edstats_figures",
                                                   "RSZO_OCC_distplot.png"),
                              xlim=(-0.5, 5)),

        PlotEdstatsDistPlot(metric='Surroundings B-factor Ratio',
                            out_dir=out_dir,
                            plot_path=os.path.join(out_dir,
                                                   "edstats_figures",
                                                   "B_ratio_distplot.png"),
                            xlim=(0.5, 2)),

        PlotEdstatsDistPlot(metric='RSZD',
                            out_dir=out_dir,
                            plot_path=os.path.join(out_dir,
                                                   "edstats_figures",
                                                   "RSZD_distplot.png"),
                            xlim=(0, 5)),
        # PlotEdstats(out_dir=out_dir)
    ],
    local_scheduler=True,
    workers=100,
)