#!/bin/bash
#PBS -joe -N

module load phenix

cd {out_dir}

giant.quick_refine {pdb} {mtz} {cif} params={params} program=phenix
giant.split_conformations {pdb}