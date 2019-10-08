import os
import pandas as pd
import numpy as np
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from mpl_toolkits.axes_grid1 import make_axes_locatable
import seaborn as sns

matplotlib.rcParams["text.usetex"] = True
# For calibiri font on label
from matplotlib import rc

rc("font", **{"family": "sans-serif", "sans-serif": ["calibri"]})


def plot_histogram_collection_bin(
    data,
    file_path,
    max=None,
    min=None,
    bin_size=None,
    bin_num=50,
    title="",
    xlabel="",
    ylabel="Frequency",
):
    """
    Histogram with collection bin for high values

    Defaults plot with 95% of data in range

    Parameters
    ----------
    data
    filename
    min: float, int
        minimal value to show
    max: float, int
        maximal value to show
    bin_size
    title
    xlabel
    ylabel

    Returns
    -------
    None
    """
    if max is None:
        max = np.percentile(data, 95)
    if min is None:
        min = np.min(data)
    if bin_size is None:
        bin_size = (max - min) / bin_num

    bins = np.arange(min, max, bin_size)

    fig, ax = plt.subplots(figsize=(9, 5))
    _, bins, patches = plt.hist(
        np.clip(data, bins[0], bins[-1]), density=True, bins=bins
    )

    short_bins = np.arange(min, max, (max - min) / 10)
    xlabels = short_bins[1:].astype(str)
    xlabels = [num[:5] for num in xlabels]

    x_pos = np.append(short_bins, max)
    x_pos = np.delete(x_pos, 0)
    final = "{:.2f}+".format(max)
    xlabels.append(final)

    ax.xaxis.set_major_locator(ticker.FixedLocator(x_pos))
    ax.xaxis.set_major_formatter(ticker.FixedFormatter(xlabels))

    plt.xlim([min, max])
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)

    fig.tight_layout()
    plt.savefig(file_path, dpi=300)
    plt.close()


def refinement_summary_plot(refinement_csv, out_file_path):

    # TODO Split across complete group/ crystal

    summary = pd.read_csv(refinement_csv, header=None, names=["comment", "total"])

    color_map = {}
    color_map["Correctly Occupied"] = "green"
    color_map["Refinement latest is dimple"] = "red"

    summary["color"] = summary.comment.map(color_map)
    summary.replace(np.nan, "orange", regex=True, inplace=True)

    summary.set_index("comment", inplace=True)

    title = "Xchem database hit refinement: {} crystals".format(summary.total.sum())
    ax = summary.plot.bar(y="total", color=summary.color, title=title, legend=False)
    ax.set_ylabel("Number of crystals [complete groups]")
    fig = ax.get_figure()
    fig.set_size_inches(10, 12)
    fig.subplots_adjust(bottom=0.2)
    fig.savefig(out_file_path, dpi=300)


def occupancy_histogram_from_state_occ(occ_correct_csv, plot_path, state):

    """
    Plot occupancy histogram

    Parameters
    ----------
    occ_conv_csv : str
        path to csv with occupancy convergence information
        for each residue involved in complete groups
    plot_path : str
        path to plot
    state: str
        "bound" or "ground"

    Returns
    -------
    None
    """

    occ_correct_df = pd.read_csv(occ_correct_csv)

    # Select out residues corresponding to the state: "ground" or "bound"
    state_df = get_state_df(occ_correct_df=occ_correct_df, state=state)

    occ = state_df["state occupancy"]
    b_factor = state_df["B_mean"]

    print(np.median(occ))
    print(np.mean(occ))
    print(np.percentile(occ, 5))
    print(np.percentile(occ, 25))
    print(np.percentile(occ, 75))

    print(np.median(b_factor))
    print(np.mean(b_factor))
    print(np.percentile(b_factor, 5))
    print(np.percentile(b_factor, 25))
    print(np.percentile(b_factor, 75))

    plt.hist(b_factor)
    plt.savefig(
        os.path.join(os.path.dirname(plot_path), "existing_refinement_b_factor.png")
    )
    plt.close()

    plot_occ_hist(occ=occ, b_factor=b_factor, out_file=plot_path)


def occupancy_histogram(occ_correct_csv, plot_path):
    """
    Plot occupancy histogram

    Parameters
    ----------
    occ_conv_csv : str
        path to csv with occupancy convergence information
        for each residue involved in complete groups
    plot_path : str
        path to plot

    Returns
    -------
    None
    """
    occ_df = pd.read_csv(occ_correct_csv)

    occ = occ_df["occupancy"]
    b_factor = occ_df["B_mean"]

    plot_occ_hist(occ=occ, b_factor=b_factor, out_file=plot_path)


def get_state_df(occ_correct_df, state):

    """
    Strip down to state occupancy and pdb, split into "bound" or "ground"

    Parameters
    ----------
    state: str
        "bound" or "ground"
    occ_correct_df: pandas.DataFrame
        dataframe that contains occupancy for

    Returns
    -------

    """

    state_df = occ_correct_df.loc[(occ_correct_df["state"] == state)]

    # Drop to unique occuopancies for each pdb
    state_df = state_df[["state occupancy", "pdb_latest", "B_mean"]]
    state_df = state_df.drop_duplicates(subset=["state occupancy", "pdb_latest"])

    return state_df


def occupancy_vs_convergence(occ_correct_csv, plot_path):
    """
    Plot occupancy vs convergence ratio of occupancy

    Parameters
    ----------
    occ_conv_csv : str
        path to csv with occupancy convergence information
        for each residue involved in complete groups
    plot_path : str
        path to plot

    Returns
    -------
    None
    """

    occ_correct_df = pd.read_csv(occ_correct_csv)

    fig, ax = plt.subplots()
    plt.scatter(x=occ_correct_df["occupancy"], y=occ_correct_df["converge"])
    plt.xlabel("Occupancy")
    plt.ylabel("Convergence ratio between " "last two point |x(n)/x(n-1)-1|")
    plt.savefig(plot_path, dpi=300)
    plt.close()


def convergence_ratio_histogram(occ_state_comment_csv, plot_path):

    """
    Plot convergence ratio of occupancy

    Parameters
    ----------
    occ_state_comment_csv : str
        path to csv with occupancy convergence information
        for each residue involved in complete groups
    plot_path : str
        path to plot

    Returns
    -------
    None
    """

    occ_correct_df = pd.read_csv(occ_state_comment_csv)

    ground_df = get_state_df(occ_correct_df=occ_correct_df, state="ground")

    plot_histogram_collection_bin(
        data=occ_correct_df["converge"],
        file_path=plot_path,
        title="Convergence of refinement: " "{} datasets".format(len(ground_df)),
        xlabel="Convergence ratio between " "last two point |x(n)/x(n-1)-1|",
    )


def convergence_ratio_kde_plot(occ_correct_csv_1, occ_correct_csv_2, plot_path):

    occ_correct_df_1 = pd.read_csv(occ_correct_csv_1)
    ground_df_1 = get_state_df(occ_correct_df=occ_correct_df_1, state="ground")

    occ_correct_df_2 = pd.read_csv(occ_correct_csv_2)
    ground_df_2 = get_state_df(occ_correct_df=occ_correct_df_2, state="ground")

    fig, ax = plt.subplots(figsize=(9, 5))

    # Seaborn distplot doesn't work, as the number of gridpoints and the scalra
    # are by default and cannot be changed in the distplot interface.
    # In particualr the large spread of the data causes this issue.
    # 'bw' is the scalr used to determine the kernel size
    # Log transformation of convergence is useful as it makes t
    # the distribution over a smaller range.
    sns.kdeplot(
        data=np.log10(occ_correct_df_1["converge"]),
        bw=0.5,
        gridsize=10000,
        label="Existing refinements REFMAC5 superposed",
        color='lightcoral',

    )

    sns.kdeplot(
        data=np.log10(occ_correct_df_2["converge"]),
        bw=0.5,
        gridsize=10000,
        label="Re-refinement REFMAC5 superposed",
        color='lightcoral',
        ls='--',
    )

    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)

    plt.xlabel(
        r"Convergence: $\log\left(\left|\frac{\textrm{Occupancy}(\textrm{Final macrocycle})}"
        r"{\textrm{Occupancy}(\textrm{Penultimate\, macrocycle})}-1\right|\right)$",
        fontsize=14,
    )
    plt.ylabel("Density", fontsize=14)
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)
    plt.legend(fontsize=12,frameon=False)
    plt.tight_layout()
    plt.savefig(plot_path)  # , dpi=300)


def plot_occ_hist(occ, b_factor, out_file):

    # get dataframe object and rename if from a state-df
    df = pd.concat([occ, b_factor], axis=1)
    df = df.rename(columns={"state occupancy": "occupancy", "B_mean": "b_factor"})

    per25 = np.percentile(df['occupancy'],25)
    per75 = np.percentile(df['occupancy'],75)
    median = np.median(df['occupancy'])

    print(f'25% {per25} 75% {per75} median {median}')

    plt.xlim(0, 1)
    plt.xlabel("Occupancy")
    plt.ylabel("Freqeuncy")

    # TODO Change to maximum vlaue, and change colour split
    min_b = 0
    max_b = 120
    delta = 10
    tmp_min_b = min_b
    tmp_max_b = min_b + delta

    occ_list = []

    cmap = matplotlib.cm.get_cmap("Spectral_r")

    colours = [
        cmap(0.0),
        cmap(0.1),
        cmap(0.2),
        cmap(0.3),
        cmap(0.4),
        cmap(0.5),
        cmap(0.6),
        cmap(0.7),
        cmap(0.8),
        cmap(0.9),
        cmap(0.95),
        cmap(1.0),
    ]

    # Set up bins of occupancy from min_b to max_b in steps of delta
    while tmp_max_b < max_b:
        df_b = df.loc[(df["b_factor"] >= tmp_min_b) & (df["b_factor"] < tmp_max_b)]
        tmp_max_b += delta
        tmp_min_b += delta
        occ_list.append(df_b.occupancy)
    else:
        df_b = df.loc[(df["b_factor"] >= max_b)]
        occ_list.append(df_b.occupancy)

    fig, ax = plt.subplots()
    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)

    norm = matplotlib.colors.Normalize(vmin=0, vmax=120)

    divider = make_axes_locatable(ax)

    ax.tick_params(axis='both', which='major', labelsize=14)
    ax.tick_params(axis='both', which='minor', labelsize=14)
    ax.set_xlim(0, 1)

    ax.set_xlabel("Occupancy", fontsize=16)
    ax.set_ylabel("Frequency", fontsize=16)

    ax.hist(occ_list, bins=25, color=colours, stacked=True)

    print("1%: {}. Min {}".format(np.percentile(occ, 1),
                                  np.min(occ)))

    ax.axvline(x=np.percentile(occ, 1),
                linestyle='--',
                color='k',
                linewidth=1.0)

    y_min, y_max = ax.get_ylim()
    if np.percentile(occ, 1) < 0.1:
        ax.text(x=np.percentile(occ, 1)+0.02,
                y=0.4*y_max,
                s="1st percentile:\n{}".format(round(np.percentile(occ, 1),2)),
                horizontalalignment='left',
                fontsize=14,
                )
        ax.text(x=np.percentile(occ, 5)-0.02,
                y=0.8*y_max,
                s="5th percentile:\n{}".format(round(np.percentile(occ, 5),2)),
                horizontalalignment='right',
                fontsize=14,
                )
        ax.axvline(x=np.percentile(occ, 5),
                   linestyle=':',
                   color='k',
                   linewidth=1.0)
    else:
        ax.text(x=np.percentile(occ, 1)-0.02,
                y=0.8*y_max,
                s="1st percentile:\n{}".format(round(np.percentile(occ, 1),2)),
                horizontalalignment='right',
                fontsize=12,
                )

    if ".png" in out_file:
        out_file = out_file.strip(".png")

    plt.tight_layout()
    plt.savefig("{}_{}.png".format(out_file, len(occ)), dpi=300)

    cax = divider.append_axes("right", size="5%", pad=0.2)

    cb1 = matplotlib.colorbar.ColorbarBase(
        cax, cmap=cmap, norm=norm, orientation="vertical"
    )

    cb1.ax.set_yticklabels(["0", "20", "40", "60", "80", "100", "$\geq 120$"], fontsize=16)
    cb1.ax.set_ylabel("B factor", Rotation=270, fontsize=16)

    plt.tight_layout()
    plt.savefig("{}_{}_colourbar.png".format(out_file, len(occ)), dpi=300)
