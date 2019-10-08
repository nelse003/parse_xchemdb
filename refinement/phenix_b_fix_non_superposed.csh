#!/bin/bash
#PBS -joe -N

module load phenix

cd {out_dir}

phenix.refine {pdb} {mtz} {cif} refinement.main.number_of_macro_cycles={ncyc} refinement.output.prefix='refine' {column_label_text} refinement.refine.strategy=individual_adp refinement.refine.adp.individual.isotropic="not chain E"