#!/bin/bash
#PBS -joe -N 

source {ccp4_path}

ccp4-python {exhaustive_multiple_sampling} {pdb} {mtz} output.out_dir='{out_dir}' exhaustive.options.lower_u_iso={lower_u_iso} exhaustive.options.upper_u_iso={upper_u_iso} exhaustive.options.vary_b={vary_b} exhaustive.options.generate_pdb=True
