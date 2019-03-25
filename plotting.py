import os
import pandas as pd
import numpy as np
import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker


def plot_histogram_collection_bin(data,
                                  file_path,
                                  max=None,
                                  min=None,
                                  bin_size=None,
                                  bin_num=50,
                                  title='',
                                  xlabel='',
                                  ylabel='Frequency'):
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
    _, bins, patches = plt.hist(np.clip(data, bins[0], bins[-1]),
                                density=True,
                                bins=bins)

    short_bins = np.arange(min, max, (max - min) / 10)
    xlabels = short_bins[1:].astype(str)
    xlabels = [num[:5] for num in xlabels]

    x_pos = np.append(short_bins, max)
    x_pos = np.delete(x_pos, 0)
    final = ("{:.2f}+".format(max))
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

    #TODO Split across complete group/ crystal

    summary = pd.read_csv(refinement_csv, header=None, names=['comment','total'])

    color_map = {}
    color_map['Correctly Occupied'] = 'green'
    color_map['Refinement latest is dimple'] = 'red'

    summary['color'] = summary.comment.map(color_map)
    summary.replace(np.nan, 'orange', regex=True, inplace=True)

    summary.set_index('comment', inplace=True)

    title = "Xchem databse hit refinement: {} crystals".format(summary.total.sum())
    ax = summary.plot.bar(y='total',color=summary.color, title=title, legend=False)
    ax.set_ylabel('Number of crystals [complete groups]')
    fig = ax.get_figure()
    fig.set_size_inches(10, 12)
    fig.subplots_adjust(bottom=0.2)
    fig.savefig(out_file_path, dpi=300)

def occupancy_histogram(occ_correct_csv, plot_path, state):

    """
    Plot ground state occupancy histogram

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

    fig, ax = plt.subplots()
    ax.grid(False)
    plt.xlabel("Occupancy")
    plt.ylabel("Frequncy")
    plt.title("{} state: {} datasets".format(len(state_df), state.capitalize()))

    plt.hist(state_df['state occupancy'], bins=100)
    plt.savefig(plot_path, dpi=300)
    plt.close()

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

    state_df = occ_correct_df.loc[
        (occ_correct_df['state'] == state)]

    # Drop to unique occuopancies for each pdb
    state_df = state_df[['state occupancy', 'pdb_latest']]
    state_df = state_df.drop_duplicates()

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
    plt.scatter(x=occ_correct_df['occupancy'],
                y=occ_correct_df['converge'])
    plt.xlabel("Occupancy")
    plt.ylabel('Convergence ratio between ' \
               'last two point |x(n)/x(n-1)-1|')
    plt.savefig(plot_path, dpi=300)
    plt.close()


def convergence_ratio_histogram(occ_correct_csv, plot_path):

    """
    Plot convergence ratio of occupancy

    Parameters
    ----------
    occ_correct_csv : str
        path to csv with occupancy convergence information
        for each residue involved in complete groups
    plot_path : str
        path to plot

    Returns
    -------
    None
    """

    occ_correct_df = pd.read_csv(occ_correct_csv)

    ground_df = get_state_df(occ_correct_df=occ_correct_df, state="ground")

    plot_histogram_collection_bin(data=occ_correct_df['converge'],
                                  file_path=plot_path,
                                  title='Convergence of refinement: ' \
                                        '{} datasets'.format(
                                      len(ground_df)),
                                  xlabel='Convergence ratio between ' \
                                         'last two point |x(n)/x(n-1)-1|')
