#!/bin/bash
#PBS -joe -N

module load phenix

phenix.refine {pdb} {mtz} {cif} refinement.main.number_of_macro_cycles={ncyc} refinement.output.prefix={out_dir}