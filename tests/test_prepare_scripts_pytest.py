import os
import pytest

from refinement.prepare_scripts import write_refmac_csh


class InputFiles:
    """
    Test parameters for write_refmac_csh
    """
    def __init__(self):
        self.resources_dir = "/dls/science/groups/i04-1/elliot-dev/parse_xchemdb/tests/resources/DCP2B-x0146"
        self.out_dir = "/dls/science/groups/i04-1/elliot-dev/parse_xchemdb/tests/output"

        self.script_dir = "/dls/science/groups/i04-1/elliot-dev/parse_xchemdb"
        self.refinement_script_dir = os.path.join(self.out_dir, "scripts")
        self.cif = os.path.join(self.resources_dir, "FMOPL000435a.cif")
        self.bound_pdb = os.path.join(self.resources_dir, "refine.split.bound-state.pdb")
        self.ground_pdb = os.path.join(self.resources_dir, "refine.split.ground-state.pdb")
        self.params = os.path.join(self.resources_dir, "input.params")
        self.free_mtz = os.path.join(self.resources_dir, "DCP2B-x0146.free.mtz")
        self.crystal = "DCP2B-x0146"
        self.ncyc = 50
        self.ccp4_path = "/dls/science/groups/i04-1/" \
                         "software/pandda_0.2.12/ccp4/ccp4-7.0/bin/" \
                         "ccp4.setup-sh"

@pytest.fixture
def setup_input_files():

    yield InputFiles()

    # shutil.rmtree(InputFiles().input_dir)


def test_write_refmac_bound(setup_input_files):
    """Test wheter bound state refmac csh file is produced"""

    write_refmac_csh(pdb=setup_input_files.bound_pdb,
                     crystal=setup_input_files.crystal,
                     cif=setup_input_files.cif,
                     mtz=setup_input_files.free_mtz,
                     out_dir=setup_input_files.out_dir,
                     refinement_script_dir=setup_input_files.refinement_script_dir,
                     script_dir=setup_input_files.script_dir,
                     ncyc=setup_input_files.ncyc,
                     ccp4_path=setup_input_files.ccp4_path)

    out_csh = os.path.join(setup_input_files.refinement_script_dir,
                         "{}_bound.csh".format(setup_input_files.crystal))

    assert os.path.isfile(out_csh)

    with open(out_csh) as f:
        lines = f.readlines()

    assert lines[3] == "source {}\n".format(setup_input_files.ccp4_path)


def test_write_refmac_ground(setup_input_files):
    """Test wheter ground state refmac csh file is produced"""

    write_refmac_csh(pdb=setup_input_files.ground_pdb,
                     crystal=setup_input_files.crystal,
                     cif=setup_input_files.cif,
                     mtz=setup_input_files.free_mtz,
                     out_dir=setup_input_files.out_dir,
                     refinement_script_dir=setup_input_files.refinement_script_dir,
                     script_dir=setup_input_files.script_dir,
                     ncyc=setup_input_files.ncyc,
                     ccp4_path=setup_input_files.ccp4_path)

    out_csh = os.path.join(setup_input_files.refinement_script_dir,
                           "{}_ground.csh".format(setup_input_files.crystal))

    assert os.path.isfile(out_csh)

    with open(out_csh) as f:
        lines = f.readlines()

    assert lines[3] == "source {}\n".format(setup_input_files.ccp4_path)