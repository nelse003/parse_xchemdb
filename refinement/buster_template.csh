module load buster

pdb2occ -p {pdb} -o {occ_params}

refine -p {pdb} -m {mtz} -l {cif} -d {out_dir} -Gelly {occ_params}