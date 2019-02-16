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

    summary = pd.read_csv(refinement_csv, header=None, names=['comment','total'])
    color_map = {}
    color_map['Correctly occupied'] = 'green'
    color_map['Refinement latest is dimple'] = 'red'
    summary['color'] = summary.comment.map(color_map)
    summary.replace(np.nan, 'orange', regex=True, inplace=True)
    summary.set_index('comment', inplace=True)
    title = "Xchem databse hit refinement: {} crystals".format(summary.total.sum())
    ax = summary.plot.bar(y='total',color=summary.color, title=title, legend=False)
    ax.set_ylabel('Number of crystals')
    fig = ax.get_figure()
    fig.set_size_inches(10, 12)
    fig.subplots_adjust(bottom=0.2)
    fig.savefig(out_file_path, dpi=300)

def main():
    occ_conv_csv = "/dls/science/groups/i04-1/elliot-dev/Work/" \
                   "exhaustive_parse_xchem_db/occ_conv.csv"

    out_dir = "/dls/science/groups/i04-1/elliot-dev/Work/" \
              "exhaustive_parse_xchem_db"

    occ_df = pd.read_csv(occ_conv_csv, index_col=[0, 1])

    occ_correct_df = occ_df[occ_df['comment'] == 'Correctly occupied']

    # Drop non refined cases
    occ_correct_no_comment_df = occ_correct_df.drop(columns=['comment', 'converge'])

    occ_correct_df['last_value'] = \
        occ_correct_no_comment_df.ffill(axis=1).iloc[:, -1]

    ground_df = occ_correct_df.loc[
        (occ_correct_df.index.get_level_values('state') == 'ground')]
    bound_df = occ_correct_df.loc[
        (occ_correct_df.index.get_level_values('state') == 'bound')]

    fig, ax = plt.subplots()
    ax.grid(False)
    plt.xlabel("Occupancy")
    plt.ylabel("Frequncy")
    plt.title("Ground State: {} datasets".format(len(ground_df)))

    plt.hist(ground_df['last_value'], bins=100)
    plt.savefig(os.path.join(out_dir, "ground_occ_hist"))
    plt.close()

    fig, ax = plt.subplots()
    plt.xlabel("Occupancy")
    plt.ylabel("Frequncy")
    plt.title("Bound State: {} datasets".format(len(bound_df)))

    plt.hist(bound_df['last_value'], bins=100)
    plt.savefig(os.path.join(out_dir, "bound_occ_hist"))
    plt.close()

    fig, ax = plt.subplots()
    plt.scatter(x=occ_correct_df['last_value'],
                y=occ_correct_df['converge'])
    plt.xlabel("Occupancy")
    plt.ylabel('Convergence ratio between ' \
               'last two point |x(n)/x(n-1)-1|')
    plt.savefig(os.path.join(out_dir, "convergence_occ"))
    plt.close()

    plot_histogram_collection_bin(data=occ_correct_df['converge'],
                                  file_path=os.path.join(out_dir,
                                                         "convergence_hist"),
                                  title='Convergence of refinement: ' \
                                        '{} datasets'.format(
                                      len(ground_df)),
                                  xlabel='Convergence ratio between ' \
                                         'last two point |x(n)/x(n-1)-1|')

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


if __name__ == "__main__":
    main()

