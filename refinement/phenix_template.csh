#!/bin/bash
#PBS -joe -N

module load phenix

cd {out_dir}

phenix.refine {pdb} {mtz} {cif} refinement.main.number_of_macro_cycles={ncyc} refinement.output.prefix='refine' {column_label_text}