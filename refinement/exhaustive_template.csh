#!/bin/bash
#PBS -joe -N 

source {ccp4_path}

mkdir {out_dir}
mkdir {crystal_dir}

ccp4-python {exhaustive_multiple_sampling} {pdb} {mtz} output.out_dir='{crystal_dir}'
