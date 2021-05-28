import glob
import os
import shutil
from pathlib import Path

from powersimdata import Scenario
from powersimdata.data_access.csv_store import CsvStore
from powersimdata.data_access.data_access import LocalDataAccess
from powersimdata.data_access.execute_list import ExecuteListManager
from powersimdata.data_access.scenario_list import ScenarioListManager
from powersimdata.utility import server_setup


class CloneCsv(CsvStore):
    def clone(self, scenario_ids, to_dir):
        table = self.get_table()
        mask = table.index.isin(scenario_ids)
        table = table.loc[mask, :]
        table.to_csv(Path(to_dir, self._FILE_NAME))


class CloneScenarioCsv(CloneCsv, ScenarioListManager):
    pass


class CloneExecuteCsv(CloneCsv, ExecuteListManager):
    pass


def copy_metadata(to_dir, scenario_ids):
    _lda = LocalDataAccess()
    CloneScenarioCsv(_lda).clone(scenario_ids, to_dir)
    CloneExecuteCsv(_lda).clone(scenario_ids, to_dir)


def download_data(scenario_ids):
    for sid in scenario_ids:
        s = Scenario(sid)
        s.get_lmp()
        s.get_load_shed()
        s.get_pf()
        s.get_pg()
        s.get_dcline_pf()
        s.get_congu()
        s.get_congl()
        s.get_averaged_cong()
        s.get_storage_e()
        s.get_storage_pg()


def get_src_dest(name, to_dir):
    folder = Path("data", name)
    src = Path(server_setup.LOCAL_DIR, folder)
    dest = Path(to_dir, folder)
    os.makedirs(dest, exist_ok=True)
    return src, dest


def copy_input(to_dir, scenario_ids):
    src, dest = get_src_dest("input", to_dir)
    for sid in scenario_ids:
        print(f"Copying input data for {sid}")
        ct = Path(src, f"{sid}_ct.pkl")
        shutil.copy(ct, dest)

        grid_mat = Path(src, f"{sid}_grid.mat")
        shutil.copy(grid_mat, dest)


def copy_output(to_dir, scenario_ids):
    src, dest = get_src_dest("output", to_dir)
    for sid in scenario_ids:
        print(f"Copying output data for {sid}")
        files = glob.glob(os.path.join(src, f"{sid}_*.pkl"))
        for f in files:
            shutil.copy(f, dest)


# example usage:
# python powersimdata/utility/clone.py CloneData 403,1171,2497,3101,3287
if __name__ == "__main__":
    import sys

    to_dir = Path(Path.home(), sys.argv[1])
    ids = sys.argv[2]
    scenario_ids = [int(sid) for sid in ids.split(",")]

    os.makedirs(to_dir, exist_ok=True)
    copy_metadata(to_dir, scenario_ids)
    # download_data(scenario_ids)
    copy_input(to_dir, scenario_ids)
    copy_output(to_dir, scenario_ids)
