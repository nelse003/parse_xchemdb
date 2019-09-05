import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib

from plotting import plot_occ_hist

## TODO Remove duplication of function
def u_iso_to_b_fac(u_iso):
    """
    Convert isotropic B-factor to

    Parameters
    ----------
    u_iso: float
        isotropic displacement (u_iso)

    Returns
    -------
    b_iso: float
        isotropic B-factor
    """
    b_iso = (8 * np.pi ** 2) * u_iso
    return b_iso


def find_all_exhaustive_minima(out_dir, in_csv, out_csv):
    """

    Parameters
    ----------
    out_dir: str
        directory containing all exhaustive hits
    in_csv: str
        format of exhaustive csv
    out_csv

    Returns
    -------

    """
    summary_df_dict = dict()
    for xtal_dir in os.listdir(out_dir):

        if os.path.isdir(os.path.join(out_dir, xtal_dir)):

            xtal_csv = os.path.join(out_dir, xtal_dir, in_csv)

            if os.path.isfile(xtal_csv):
                df = pd.read_csv(
                    xtal_csv,
                    header=None,
                    names=["bound_occupancy",
                           "ground_occupancy",
                           "u_iso",
                           "fo_fc"],
                )
                # print(df)
                index_to_drop = df[(df.bound_occupancy > 1.00)].index
                df.drop(index_to_drop,
                        inplace=True)

                xtal = xtal_dir
                occ = df.loc[df["fo_fc"].idxmin()]["bound_occupancy"]
                u_iso = df.loc[df["fo_fc"].idxmin()]["u_iso"]
                b_factor = u_iso_to_b_fac(u_iso)

                summary_df_dict[xtal] = [occ, b_factor]

    summary_df = pd.DataFrame.from_dict(
        summary_df_dict, orient="index", columns=["occupancy", "b_factor"]
    )
    summary_df.to_csv(os.path.join(out_dir, out_csv))


def plot_scatter_exhaustive_minima(out_csv, out_file):

    df = pd.read_csv(out_csv)
    occ = df.occupancy
    b_factor = df.b_factor

    plt.scatter(occ, b_factor)
    plt.savefig(out_file)


def plot_hist_exhaustive_minima(out_csv, out_file):

    df = pd.read_csv(out_csv)
    occ = df.occupancy
    b_factor = df.b_factor
    plot_occ_hist(occ=occ, b_factor=b_factor, out_file=out_file)
