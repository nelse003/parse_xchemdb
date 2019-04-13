import os
from utils.filesystem import check_inputs
from utils.filesystem import find_program_from_parameter_file

# From first line of log_pdb_mtz.
# TODO Replace with local data included in repo, or gettable by curl

def test_check_inputs_refmac():

    """Test check inputs with no parameter file specified,
    and refmac as a program."""

    cif = "/dls/labxchem/data/2017/lb13385-86/processing/ACVR1A-x1242/compound/Z1929757385.cif"
    pdb = "/dls/labxchem/data/2017/lb13385-86/processing/analysis/initial_model/ACVR1A-x1242/Refine_0003/refine_3.pdb"
    params = ''
    free_mtz = "/dls/labxchem/data/2017/lb13385-86/processing/analysis/initial_model/ACVR1A-x1242/ACVR1A-x1242.free.mtz"
    input_dir = "/dls/science/groups/i04-1/elliot-dev/Work/exhaustive_parse_xchem_db/test/test_filesystem"
    crystal = "ACVR1A-x1242"

    cif, params, free_mtz = check_inputs(cif=cif, pdb=pdb, params=params, free_mtz=free_mtz,
                                         refinement_program="refmac", input_dir=input_dir, crystal=crystal)
    assert os.path.isfile(cif)
    assert os.path.isfile(params)
    assert os.path.isfile(free_mtz)
    assert find_program_from_parameter_file(params) == "refmac"

def test_check_inputs_phenix():

    """Test check inputs with no parameter file specified,
    and phenix as a program."""

    cif = "/dls/labxchem/data/2017/lb13385-86/processing/ACVR1A-x1242/compound/Z1929757385.cif"
    pdb = "/dls/labxchem/data/2017/lb13385-86/processing/analysis/initial_model/ACVR1A-x1242/Refine_0003/refine_3.pdb"
    params = ''
    free_mtz = "/dls/labxchem/data/2017/lb13385-86/processing/analysis/initial_model/ACVR1A-x1242/ACVR1A-x1242.free.mtz"
    input_dir = "/dls/science/groups/i04-1/elliot-dev/Work/exhaustive_parse_xchem_db/test/test_filesystem"
    crystal = "ACVR1A-x1242"

    cif, params, free_mtz = check_inputs(cif=cif, pdb=pdb, params=params, free_mtz=free_mtz,
                                         refinement_program="phenix", input_dir=input_dir, crystal=crystal)
    assert os.path.isfile(cif)
    assert os.path.isfile(params)
    assert os.path.isfile(free_mtz)
    assert find_program_from_parameter_file(params) == "phenix"