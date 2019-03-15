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
    plt.savefig(file_path)
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

def state_occ(row, bound, ground, pdb):
    if row.pdb_latest == pdb:
        if row.state == "bound":
            return bound
        if row.state == "ground":
            return ground

def prepare_df_from_occ_conv_csv(occ_conv_csv):

    """
    Parse occupancy convergence csv into multiple

    Parameters
    ----------
    occ_conv_csv: str
        path to csv with occupancy convergence information
        for each residue involved in complete groups

    Returns
    -------
    ground_df: pandas.DataFrame
        DataFrame containing correctly occupied ground state residues
        occupancy convergence information
    bound_df: pandas.DataFrame
        DataFrame containing correctly occupied ground state residues
        occupancy convergence information
    occ_correct_df: pandas.DataFrame
        DataFrame containing correctly occupied residues
    """
    # Read CSV
    occ_df = pd.read_csv(occ_conv_csv, index_col=[0, 1])

    # Select only residues that are correctly occupied
    occ_correct_df = occ_df[occ_df['comment'] == 'Correctly Occupied']

    # print(occ_correct_df.head())
    # print(occ_correct_df.columns.values)

    # TODO Fix to run whre rows are different lengths

    int_cols = []
    for col in occ_correct_df.columns.values:
        try:
            int_cols.append(int(col))
        except ValueError:
            continue
    str_cols = list(map(str,int_cols))
    df = occ_correct_df[str_cols]

    occ_correct_df['converge'] = abs(df[str_cols[-1]] / df[str_cols[-2]] - 1)

    # TODO Move to new luigi.Task

    # Select the final occupancy value
    occ_correct_df['occupancy'] = df[str_cols[-1]]

    pdb_df_list = []
    for pdb in occ_correct_df.pdb_latest.unique():

        bound = 0
        ground = 0

        pdb_df = occ_correct_df.loc[
        (occ_correct_df['pdb_latest'] == pdb)]

        grouped = pdb_df.groupby(['complete group','occupancy','alte','state'])

        for name, group in grouped:

            group_occ = group.occupancy.unique()[0]

            if "ground" in group.state.unique()[0]:
                ground += group_occ

            if "bound" in group.state.unique()[0]:
                bound += group_occ

        print(ground + bound)
        try:
            np.testing.assert_allclose(ground + bound, 1.0, atol=0.01)
        except AssertionError:
            continue

        pdb_df['state occupancy'] = pdb_df.apply(
            func=state_occ,
            bound=bound,
            ground=ground,
            pdb=pdb,
            axis=1)

        pdb_df_list.append(pdb_df)

    occ_correct_df = pd.concat(pdb_df_list)

    occ_correct_df.to_csv("/dls/science/groups/i04-1/elliot-dev/Work/" \
              "exhaustive_parse_xchem_db/occ_correct.csv")

    # Select out ground state residues
    ground_df = occ_correct_df.loc[
        (occ_correct_df['state'] == 'ground')]

    # Select out bound state residues
    bound_df = occ_correct_df.loc[
        (occ_correct_df['state'] == 'bound')]

    return ground_df, bound_df, occ_correct_df

def ground_state_occupancy_histogram(occ_conv_csv, plot_path):

    """
    Plot ground state occupancy histogram

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

    ground_df, bound_df, occ_correct_df = prepare_df_from_occ_conv_csv(occ_conv_csv)

    fig, ax = plt.subplots()
    ax.grid(False)
    plt.xlabel("Occupancy")
    plt.ylabel("Frequncy")
    plt.title("Ground State: {} datasets".format(len(ground_df)))

    plt.hist(ground_df['state occupancy'], bins=100)
    plt.savefig(plot_path)
    plt.close()

def bound_state_occ_histogram(occ_conv_csv, plot_path):

    """
    Plot bound state occupancy histogram

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
    occ_correct_df = pd.read_csv("/dls/science/groups/i04-1/elliot-dev/Work/" \
                                  "exhaustive_parse_xchem_db/occ_correct.csv")

    # Select out bound state residues
    bound_df = occ_correct_df.loc[
        (occ_correct_df['state'] == 'bound')]

    bound_df = bound_df[['state occupancy','pdb_latest']]

    print(bound_df)
    print(len(bound_df))

    bound_df = bound_df.drop_duplicates()

    print(bound_df)
    print(len(bound_df))

    fig, ax = plt.subplots()
    plt.xlabel("Occupancy")
    plt.ylabel("Frequncy")
    plt.title("Bound State: {} datasets".format(len(bound_df)))

    plt.hist(bound_df['state occupancy'], bins=100)
    plt.savefig(plot_path)
    plt.close()

def occupancy_vs_convergence(occ_conv_csv, plot_path):
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

    ground_df, bound_df, occ_correct_df = prepare_df_from_occ_conv_csv(occ_conv_csv)

    fig, ax = plt.subplots()
    plt.scatter(x=occ_correct_df['occupancy'],
                y=occ_correct_df['converge'])
    plt.xlabel("Occupancy")
    plt.ylabel('Convergence ratio between ' \
               'last two point |x(n)/x(n-1)-1|')
    plt.savefig(plot_path)
    plt.close()

def convergence_ratio_histogram(occ_conv_csv, plot_path):

    """
    Plot convergence ratio of occupancy

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

    ground_df, bound_df, occ_correct_df = prepare_df_from_occ_conv_csv(occ_conv_csv)

    plot_histogram_collection_bin(data=occ_correct_df['converge'],
                                  file_path=plot_path,
                                  title='Convergence of refinement: ' \
                                        '{} datasets'.format(
                                      len(ground_df)),
                                  xlabel='Convergence ratio between ' \
                                         'last two point |x(n)/x(n-1)-1|')


def example_convergence():
    """ NOT WORKING"""

    for index, row in occ_correct_no_comment_df.iterrows():
        if index[0] == "PDK2-x0856":
            plt.plot(row, marker='o', linestyle='dashed')
    plt.title("PDK2-x0856")
    plt.xlabel("Occupancy refinement cycles")
    plt.ylabel("Occupancy")
    plt.savefig(os.path.join(out_dir, "example_convergence"))
    plt.close()

    for index, row in occ_correct_no_comment_df.iterrows():
        if index[0] == "NUDT7A_Crude-x1435":
            plt.plot(row, marker='o', linestyle='dashed')
    plt.title("NUDT7A_Crude-x1435")
    plt.xlabel("Occupancy refinement cycles")
    plt.ylabel("Occupancy")
    plt.savefig(os.path.join(out_dir, "example_convergence_1"))
    plt.close()

    for index, row in occ_correct_no_comment_df.iterrows():
        if index[0] == "ACVR1A-x1279":
            plt.plot(row, marker='o', linestyle='dashed')
    plt.title("ACVR1A-x1279")
    plt.xlabel("Occupancy refinement cycles")
    plt.ylabel("Occupancy")
    plt.savefig(os.path.join(out_dir, "example_convergence_2"))
    plt.close()




