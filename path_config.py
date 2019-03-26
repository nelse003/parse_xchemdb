import os
import luigi

class Path(luigi.Config):

    """Config: Paths to be used """

    # Default Directories

    # Directory containing scripts (i.e. repo)
    script_dir = "/dls/science/groups/i04-1/elliot-dev/parse_xchemdb"

    # Output directory, where default paths are derived from
    out_dir = "/dls/science/groups/i04-1/elliot-dev/Work/" \
              "exhaustive_parse_xchem_db/"

    # Temporary directory to contain csh scripts
    tmp_dir = os.path.join(out_dir, "tmp")

    refinement_dir = os.path.join(out_dir, "convergence_refinement")

    # Directory to contain bound refinements (refmac)
    bound_refinement_dir = "/dls/science/groups/i04-1/elliot-dev/Work/" \
                        "exhaustive_parse_xchem_db/bound_refinement"

    # Directory to contain ground refinements (refmac)
    ground_refinement_dir = "/dls/science/groups/i04-1/elliot-dev/Work/" \
                        "exhaustive_parse_xchem_db/ground_refinement"

    # Csvs from parsing the database
    log_pdb_mtz = luigi.Parameter(
        default=os.path.join(out_dir, 'log_pdb_mtz.csv'))
    log_occ = luigi.Parameter(
        default=os.path.join(out_dir, 'log_occ.csv'))
    log_occ_resname = luigi.Parameter(
        default=os.path.join(out_dir, 'log_occ_resname.csv'))

    occ_conv = luigi.Parameter(
        default=os.path.join(out_dir, 'occ_conv.csv'))
    refinement_summary = luigi.Parameter(
        default=os.path.join(out_dir, 'refinement_summary.csv'))
    refine = luigi.Parameter(
        default=os.path.join(out_dir, 'refinement.csv'))
    superposed = luigi.Parameter(
        default=os.path.join(out_dir, 'superposed.csv'))
    occ_conv_failures = luigi.Parameter(
        default=os.path.join(out_dir, 'occ_conv_failures.csv'))

    # Csv paths from parsing the re-refinements done to extend towards convergence
    convergence_refinement_failures = luigi.Parameter(
        default=os.path.join(out_dir, 'convergence_refinement.csv'))

    convergence_refinement = luigi.Parameter(
        default=os.path.join(out_dir, 'convergence_refinement_log_pdb_mtz.csv'))

    convergence_occ = luigi.Parameter(
        default=os.path.join(out_dir, 'convergence_refinement_occ.csv'))

    convergence_occ_resname = luigi.Parameter(
        default=os.path.join(out_dir, 'convergence_refinement_occ_resname.csv'))

    convergence_occ_conv = luigi.Parameter(
        default=os.path.join(out_dir, 'convergence_refinement_occ_conv.csv'))

    convergence_occ_correct = luigi.Parameter(
        default=os.path.join(out_dir, "occ_correct.csv"))

    # Plot paths
    refinement_summary_plot = luigi.Parameter(
        default=os.path.join(out_dir, 'refinement_summary.png'))
    bound_occ_hist = luigi.Parameter(
        default=os.path.join(out_dir, 'bound_occ_hist.png'))

    # Plots convergence
    convergence_bound_hist = luigi.Parameter(
        default=os.path.join(out_dir, 'convergence_bound_occ_hist.png'))
    convergence_ground_hist = luigi.Parameter(
        default=os.path.join(out_dir, 'convergence_ground_occ_hist.png'))
    convergence_occ_conv_scatter = luigi.Parameter(
        default=os.path.join(out_dir, 'convergence_occ_conv_scatter.png'))
    convergence_conv_hist = luigi.Parameter(
        default=os.path.join(out_dir, 'convergence_conv_hist.png'))


    # Bound refmac refinement
    bound_refinement = luigi.Parameter(
        default=os.path.join(out_dir, 'bound_refinement_log_pdb_mtz.csv'))

    bound_occ = luigi.Parameter(
        default=os.path.join(out_dir, 'bound_refinement_occ.csv'))

    bound_occ_conv_state = luigi.Parameter(
        default=os.path.join(out_dir, 'bound_refinement_occ_conv_state.csv'))


    # Bound refmac refinement plotting
    bound_refmac_bound_hist = luigi.Parameter(
        default=os.path.join(out_dir, 'bound_refmac_bound_hist.png'))


    # Batch management csvs
    bound_refinement_batch_csv = luigi.Parameter(
        default=os.path.join(out_dir,'bound_refmac.csv'))

    ground_refinement_batch_csv = luigi.Parameter(
        default=os.path.join(out_dir,'bound_refmac.csv'))

    # Directory Luigi Parameters
    # Currently derived from above default values.
    # TODO alternative ways fo defining directories

    # Directory containing scripts (i.e. repo)
    script_dir = luigi.Parameter(default= script_dir)

    # Temporary directory to contain csh scripts
    tmp_dir = luigi.Parameter(default= tmp_dir)

    # Output directory, where default paths are derived from
    out_dir = luigi.Parameter(default=out_dir)

    refinement_dir = luigi.Parameter(default=refinement_dir)

    # Directory to contain bound refinements (refmac)
    bound_refinement_dir = luigi.Parameter(default=bound_refinement_dir)

    # Directory to contain ground refinements (refmac)
    ground_refinement_dir = luigi.Parameter(default=ground_refinement_dir)

    ccp4 = luigi.Parameter(default="/dls/science/groups/i04-1/" \
                             "software/pandda_0.2.12/ccp4/ccp4-7.0/bin/" \
                             "ccp4.setup-sh")