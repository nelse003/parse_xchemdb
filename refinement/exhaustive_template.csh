#!/bin/bash
#PBS -joe -N 

source {ccp4_path}

ccp4-python {exhaustive_multiple_sampling} {pdb} {mtz} output.out_dir='{out_dir}'
