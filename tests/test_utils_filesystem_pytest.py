import os
import pytest
import shutil
from utils.filesystem import check_inputs
from utils.filesystem import find_program_from_parameter_file


class InputFiles:
    """
    Test parameters for check_input
    """
    def __init__(self):
        self.resources_dir = "/dls/science/groups/i04-1/elliot-dev/parse_xchemdb/tests/resources/DCP2B-x0146"
        self.out_dir = "/dls/science/groups/i04-1/elliot-dev/parse_xchemdb/tests/output"

        self.cif = os.path.join(self.resources_dir, "FMOPL000435a.cif")
        self.pdb = os.path.join(self.resources_dir, "refine.pdb")
        self.params = os.path.join(self.resources_dir, "input.params")
        self.free_mtz = os.path.join(self.resources_dir, "DCP2B-x0146.free.mtz")
        self.crystal = "DCP2B-x0146"

@pytest.fixture(scope='class')
def setup_input_files(request):

    request.cls.input_files = InputFiles()
    yield
    shutil.rmtree(InputFiles().out_dir)

@pytest.mark.usefixtures('setup_input_files')
class TestCheckInputs():

    def test_check_inputs_refmac_empty_params(self):


        """Test check inputs with no parameter file specified,
        and refmac as a program."""

        cif, params, free_mtz = check_inputs(cif=self.input_files.cif,
                                             pdb=self.input_files.pdb,
                                             params="",
                                             free_mtz=self.input_files.free_mtz,
                                             refinement_program="refmac",
                                             input_dir=self.input_files.out_dir,
                                             crystal=self.input_files.crystal)

        assert os.path.isfile(cif)
        assert os.path.isfile(params)
        assert os.path.isfile(free_mtz)
        assert find_program_from_parameter_file(params) == "refmac"

    def test_check_inputs_refmac(self):

        """Test check inputs with correct parameter file specified,
        and refmac as a program."""

        cif, params, free_mtz = check_inputs(cif=self.input_files.cif,
                                             pdb=self.input_files.pdb,
                                             params=self.input_files.params,
                                             free_mtz=self.input_files.free_mtz,
                                             refinement_program="refmac",
                                             input_dir=self.input_files.out_dir,
                                             crystal=self.input_files.crystal)

        assert os.path.isfile(cif)
        assert os.path.isfile(params)
        assert os.path.isfile(free_mtz)
        assert find_program_from_parameter_file(params) == "refmac"


    def test_check_inputs_phenix_incorrect_params(self):

        """Test check inputs with incorrect parameter file specified,
        and refmac as a program."""

        cif, params, free_mtz = check_inputs(cif=self.input_files.cif,
                                             pdb=self.input_files.pdb,
                                             params=self.input_files.params,
                                             free_mtz=self.input_files.free_mtz,
                                             refinement_program="phenix",
                                             input_dir=self.input_files.out_dir,
                                             crystal=self.input_files.crystal)

        assert os.path.isfile(cif)
        assert os.path.isfile(params)
        assert os.path.isfile(free_mtz)
        assert find_program_from_parameter_file(params) == "phenix"


    def test_check_inputs_phenix(self):

        """Test check inputs with no parameter file specified,
        and phenix as a program."""

        cif, params, free_mtz = check_inputs(cif=self.input_files.cif,
                                             pdb=self.input_files.pdb,
                                             params="",
                                             free_mtz=self.input_files.free_mtz,
                                             refinement_program="phenix",
                                             input_dir=self.input_files.out_dir,
                                             crystal=self.input_files.crystal)
        assert os.path.isfile(cif)
        assert os.path.isfile(params)
        assert os.path.isfile(free_mtz)
        assert find_program_from_parameter_file(params) == "phenix"

