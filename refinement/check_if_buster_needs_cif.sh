#!/bin/sh
version="Time-stamp: <2019-08-01 09:50:03 vonrhein>"
contact="buster-develop"
#
# Copyright  2019 by Global Phasing Limited
#
#           All rights reserved.
#
#           This software is proprietary to and embodies the confidential
#           technology of Global Phasing Limited (GPhL). Possession, use,
#           duplication or dissemination of the software is authorised
#           only pursuant to a valid written licence from GPhL.
#
# Author    (2019) Clemens Vonrhein
#
# Contact   $contact@GlobalPhasing.com
#
#----------------------------------------------------------------------
#              BEGIN OF USER INPUT
#----------------------------------------------------------------------

# some generic and useful settings:
cdir=`pwd`
tmp=/tmp/`whoami`-`date +%s`_$$
exes="awk grep"
files=""
iverb=0

dics="$BDG_home/tnt/data/protgeo_eh99.dat $BDG_home/tnt/data/exoticaa.dat $BDG_home/tnt/data/nuclgeo.dat  $BDG_home/tnt/data/othergeo.dat"

#----------------------------------------------------------------------
#               END OF USER INPUT
#----------------------------------------------------------------------
echo " " >&2
echo " ============================================================================ " >&2
echo " " >&2
echo " Copyright (C) 2019 by Global Phasing Limited" >&2
echo " " >&2
echo "           All rights reserved." >&2
echo " " >&2
echo "           This software is proprietary to and embodies the confidential" >&2
echo "           technology of Global Phasing Limited (GPhL). Possession, use," >&2
echo "           duplication or dissemination of the software is authorised" >&2
echo "           only pursuant to a valid written licence from GPhL." >&2
echo " " >&2
echo " ---------------------------------------------------------------------------- " >&2
echo " " >&2
echo " Author:   (2019) Clemens Vonrhein" >&2
echo " " >&2
echo " Contact:  $contact@GlobalPhasing.com" >&2
echo " " >&2
ShortVersion=`echo $version | cut -f2- -d':' | sed "s/ [a-z0-9][a-z0-9][a-z0-9]*>/>/g"`
echo " Program:  `basename $0`   version ${ShortVersion} " >&2
echo " " >&2
echo " ============================================================================ " >&2
echo " " >&2
#----------------------------------------------------------------------
#               BEGIN OF SCRIPT
#----------------------------------------------------------------------

# --------- functions
error () {
  echo " "
  [ $# -ne 0 ] && echo " ERROR: $@" || echo " ERROR: see above"
  echo " "
  exit 1
}
warning () {
  echo " "
  [ $# -ne 0 ] && echo " WARNING: $@" || echo " WARNING: see above"
  echo " "
}
note () {
  if [ $# -gt 0 ]; then
    echo " "
    echo " NOTE: $@"
    echo " "
  fi
}
usage () {
  echo " "
  echo " USAGE: $0 [-h] [-v] <PDB>"
  echo " "
  echo "        -h                    : show help"
  echo " "
  echo "        -v                    : increase verbosity (default = $iverb)"
  echo " "
  echo "        <PDB>                 : PDB file to check"
  echo " "
}
chkarg () {
  __a=$1
  __m=$2
  __n=$3
  if [ $__n -lt $__m ]; then
    usage
    error "not enough arguments for command-line flag \"$__a\""
  fi
}

# --------- process command-line
vars=""
pdb=""
while [ $# -gt 0 ]
do
  case $1 in
    -v) iverb=`expr $iverb + 1`;;
    -h) usage; exit 0;;
    *.pdb|*.ent) pdb=$1;;
     *) usage;error "unknown argument \"$1\"";;
  esac
  shift
done

# --------- checks
for var in $vars
do
  eval "val=\$$var"
  [ "X$val" = "X" ] && usage && error "variable \"$var\" not set"
done
for exe in $exes
do
  type $exe >/dev/null 2>&1
  [ $? -ne 0 ] && error "executable \"$exe\" not found (in PATH)"
done
for f in $files $pdb $dics
do
  [ ! -f $f ] && error "file \"$f\" not found"
  [ ! -r $f ] && error "file \"$f\" not readable"
done

[ "X$BDG_home" = "X" ] && error "need to have BUSTER set-up correctly"
[ ! -d $BDG_home/tnt/data ] && error "\$BDG_home=$BDG_home doesn't seem to be a BUSTER installation directory?"

[ "X$pdb" = "X" ] && usage && error "no PDB file given"

standard_AA="ALA ARG ASN ASP CYS GLN GLU GLY HIS ILE LEU LYS MET MSE PHE PRO SER THR TRP TYR VAL"
standard_NUC="A C G I T U DA DC DG DI DT"
standard_other="HOH HEC HEM"

# --------- start doing something

# find all multi-atom residues
ress=`awk '/^ATOM|^HETATM/{
  id=substr($0,17,11)
  n[id]++
}END{
  for(id in n) {
    if (n[id]>1) {m[substr(id,2,3)]++}
  }
  for(id in m) print id
}' $pdb | sort -u`

n=0
for res in $ress
do
  [ -f $BDG_home/tnt/data/common-compounds/$res.cif ] && continue
  [ `echo " $standard_AA $standard_NUC $standard_other " | grep -c " $res "` -ne 0 ] && continue
  [ `grep -l "^GEOMETRY [ ]*$res [ ]*BOND" $dics | wc -l` -ne 0 ] && continue
  n=`expr $n + 1`
  [ $n -eq 1 ] && note "PDB file $pdb contains some residues for which there is no distributed dictionary with BUSTER ($BDG_home)"
  echo " will need dictionary for residue $res"
done

[ $n -eq 0 ] && note "good - all residues are expected to have default dictionaries provided by BUSTER ($BDG_home)"

# --------- finish
rm -fr ${tmp}*
echo " " >&2
echo " ... normal termination ... " >&2
echo " " >&2
exit 0
#----------------------------------------------------------------------
#               END OF SCRIPT
#----------------------------------------------------------------------
