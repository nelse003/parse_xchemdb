#!/bin/bash
#PBS -joe -N 

source {ccp4_path}
refmac5 HKLIN {mtz} \
HKLOUT {out_mtz} \
XYZIN {pdb} \
XYZOUT {out_pdb} \
LIBIN {cif} \
LIBOUT {out_cif} \
<< EOF > {log}

make -
    hydrogen ALL -
    hout NO -
    peptide NO -
    cispeptide YES -
    ssbridge YES -
    symmetry YES -
    sugar YES -
    connectivity NO -
    link NO
refi -
    type REST -
    resi MLKF -
    meth CGMAT -
    bref ISOT
ncyc {ncyc}
scal -
    type SIMP -
    LSSC -
    ANISO -
    EXPE
weight matrix 0.25
solvent YES
monitor MEDIUM -
    torsion 10.0 -
    distance 10.0 -
    angle 10.0 -
    plane 10.0 -
    chiral 10.0 -
    bfactor 10.0 -
    bsphere 10.0 -
    rbond 10.0 -
    ncsr 10.0
labin  FP=F SIGFP=SIGF FREE=FreeR_flag
labout  FC=FC FWT=FWT PHIC=PHIC PHWT=PHWT DELFWT=DELFWT PHDELWT=PHDELWT FOM=FOM
{occ_group}
DNAME {crystal}
END
EOF
