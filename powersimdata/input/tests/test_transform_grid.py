import copy
import numpy as np

from powersimdata.input.grid import Grid
from powersimdata.input.change_table import ChangeTable
from powersimdata.input.transform_grid import TransformGrid

grid = Grid(['USA'])
ct = ChangeTable(grid)


def get_plant_id(zone_id, gen_type):
    plant_id = grid.plant.groupby(
        ['zone_id', 'type']).get_group(
        (zone_id, gen_type)).index.values.tolist()
    return plant_id


def get_branch_id(zone_id):
    branch_id = grid.branch.groupby(
        ['from_zone_id', 'to_zone_id']).get_group(
        (zone_id, zone_id)).index.values.tolist()
    return branch_id


def test_that_only_capacities_are_modified_when_scaling_renewable_gen():
    gen_type = 'solar'
    zone = 'Utah'
    factor = 1.41
    ct.scale_plant_capacity(gen_type, zone_name={zone: factor})
    new_grid = TransformGrid(grid, ct.ct).get_grid()

    ref_grid = copy.deepcopy(grid)
    plant_id = get_plant_id(grid.zone2id[zone], gen_type)
    try:
        assert new_grid != ref_grid
        ref_grid.plant.loc[plant_id, 'Pmax'] *= factor
        ref_grid.plant.loc[plant_id, 'Pmin'] *= factor
        assert new_grid == ref_grid
    finally:
        ct.clear()
        del ref_grid


def test_scale_gen_capacity_one_zone():
    gen_type = 'coal'
    zone = 'Colorado'
    factor = 2.0
    ct.scale_plant_capacity(gen_type, zone_name={zone: factor})
    new_grid = TransformGrid(grid, ct.ct).get_grid()
    plant_id = get_plant_id(grid.zone2id[zone], gen_type)

    pmax = grid.plant.Pmax
    new_pmax = new_grid.plant.Pmax

    try:
        assert new_grid != grid
        assert not new_pmax.equals(factor * pmax)
        assert new_pmax.loc[plant_id].equals(factor * pmax.loc[plant_id])
    finally:
        ct.clear()


def test_scale_thermal_gen_gencost_two_types_two_zones():
    gen_type = ['ng', 'coal']
    zone = ['Louisiana', 'Montana Eastern']
    factor = [0.8, 1.25]
    for i, r in enumerate(gen_type):
        ct.scale_plant_capacity(r, zone_name={zone[i]: factor[i]})
    new_grid = TransformGrid(grid, ct.ct).get_grid()
    plant_id = []
    for z, r in zip(zone, gen_type):
        plant_id.append(get_plant_id(grid.zone2id[z], r))

    c0 = grid.gencost['before'].c0
    new_c0 = new_grid.gencost['before'].c0
    c1 = grid.gencost['before'].c1
    new_c1 = new_grid.gencost['before'].c1
    c2 = grid.gencost['before'].c2
    new_c2 = new_grid.gencost['before'].c2

    try:
        assert new_grid != grid
        assert new_c1.equals(c1)
        for f, i in zip(factor, plant_id):
            assert new_c0.loc[i].equals(f * c0.loc[i])
            assert new_c2.loc[i].equals(c2.loc[i] / f)
    finally:
        ct.clear()


def test_scale_renewable_gen_gencost_one_zone():
    ct.scale_plant_capacity('wind', zone_name={'Washington': 2.3})
    new_grid = TransformGrid(grid, ct.ct).get_grid()

    try:
        assert new_grid != grid
        assert new_grid.gencost['before'].c0.equals(grid.gencost['before'].c0)
        assert new_grid.gencost['before'].c1.equals(grid.gencost['before'].c1)
        assert new_grid.gencost['before'].c2.equals(grid.gencost['before'].c2)
    finally:
        ct.clear()


def test_scale_gen_one_plant():
    plant_id = 3000
    gen_type = grid.plant.loc[plant_id].type
    factor = 0.33
    ct.scale_plant_capacity(gen_type, plant_id={plant_id: factor})
    new_grid = TransformGrid(grid, ct.ct).get_grid()

    pmax = grid.plant.Pmax
    new_pmax = new_grid.plant.Pmax
    pmin = grid.plant.Pmin
    new_pmin = new_grid.plant.Pmin

    try:
        assert new_grid != grid
        assert not new_pmax.equals(factor * pmax)
        assert not new_pmin.equals(factor * pmin)
        assert new_pmax.loc[plant_id] == factor * pmax.loc[plant_id]
        assert new_pmin.loc[plant_id] == factor * pmin.loc[plant_id]

        if gen_type in ['coal', 'dfo', 'geothermal', 'ng', 'nuclear']:
            c0 = grid.gencost['before'].c0
            new_c0 = new_grid.gencost['before'].c0
            assert not new_c0.equals(factor * c0)
            assert new_c0.loc[plant_id] == factor * c0.loc[plant_id]

            c1 = grid.gencost['before'].c1
            new_c1 = new_grid.gencost['before'].c1
            assert new_c1.equals(c1)

            c2 = grid.gencost['before'].c2
            new_c2 = new_grid.gencost['before'].c2
            assert not new_c2.equals(c2 / factor)
            assert new_c2.loc[plant_id] == c2.loc[plant_id] / factor
    finally:
        ct.clear()


def test_scale_branch_one_zone():
    factor = 4
    zone = 'Washington'
    ct.scale_branch_capacity(zone_name={'Washington': factor})
    new_grid = TransformGrid(grid, ct.ct).get_grid()

    branch_id = get_branch_id(grid.zone2id[zone])

    capacity = grid.branch.rateA
    new_capacity = new_grid.branch.rateA
    x = grid.branch.x
    new_x = new_grid.branch.x

    try:
        assert new_grid != grid
        assert not new_capacity.equals(factor * capacity)
        assert new_capacity.loc[branch_id].equals(
            factor * capacity.loc[branch_id])
        assert not new_x.equals(x / factor)
        assert new_x.loc[branch_id].equals(x.loc[branch_id] / factor)
    finally:
        ct.clear()


def test_scale_branch_two_zones():
    factor = [0.3, 1.25]
    zone = ['West Virginia', 'Nevada']
    ct.scale_branch_capacity(zone_name={z: f for z, f in zip(zone, factor)})
    new_grid = TransformGrid(grid, ct.ct).get_grid()

    branch_id = []
    for z in zone:
        branch_id.append(get_branch_id(grid.zone2id[z]))

    capacity = grid.branch.rateA
    new_capacity = new_grid.branch.rateA
    x = grid.branch.x
    new_x = new_grid.branch.x

    try:
        assert new_grid.plant.equals(grid.plant)
        for f, i in zip(factor, branch_id):
            assert new_capacity.loc[i].equals(f * capacity.loc[i])
            assert new_x.loc[i].equals(x.loc[i] / f)
    finally:
        ct.clear()


def test_scale_one_branch():
    branch_id = 11111
    factor = 1.62
    ct.scale_branch_capacity(branch_id={branch_id: factor})
    new_grid = TransformGrid(grid, ct.ct).get_grid()

    capacity = grid.branch.rateA
    new_capacity = new_grid.branch.rateA
    x = grid.branch.x
    new_x = new_grid.branch.x

    try:
        assert new_grid != grid
        assert new_grid.dcline.equals(grid.dcline)
        assert not new_capacity.equals(factor * capacity)
        assert new_capacity.loc[branch_id] == factor * capacity.loc[branch_id]
        assert not new_x.equals(x / factor)
        assert new_x.loc[branch_id] == x.loc[branch_id] / factor
    finally:
        ct.clear()


def test_scale_dcline():
    dcline_id = [2, 4, 6]
    factor = [1.2, 1.6, 0]
    ct.scale_dcline_capacity({i: f for i, f in zip(dcline_id, factor)})
    new_grid = TransformGrid(grid, ct.ct).get_grid()

    pmin = grid.dcline.Pmin
    new_pmin = new_grid.dcline.Pmin
    pmax = grid.dcline.Pmax
    new_pmax = new_grid.dcline.Pmax
    status = grid.dcline.status
    new_status = new_grid.dcline.status

    try:
        assert new_grid != grid
        assert not new_status.equals(status)
        assert not new_pmin.equals(pmin)
        assert not new_pmax.equals(pmax)
        for i, f in zip(dcline_id, factor):
            assert new_pmin.loc[i] == f * pmin.loc[i]
            assert new_pmax.loc[i] == f * pmax.loc[i]
            assert status.loc[i] == 1
            assert new_status.loc[i] == 0 if f == 0 else 1
    finally:
        ct.clear()


def test_add_branch():
    new_branch = [
        {'capacity': 150, 'from_bus_id': 8, 'to_bus_id': 100},
        {'capacity': 250, 'from_bus_id': 8000, 'to_bus_id': 30000},
        {'capacity': 50, 'from_bus_id': 1, 'to_bus_id': 655},
        {'capacity': 125, 'from_bus_id': 3001005, 'to_bus_id': 3008157}]
    ct.add_branch(new_branch)
    new_grid = TransformGrid(grid, ct.ct).get_grid()

    new_capacity = new_grid.branch.rateA.values

    try:
        assert new_grid.branch.shape[0] != grid.branch.shape[0]
        assert np.array_equal(new_capacity[-len(new_branch):],
                              np.array([ac['capacity'] for ac in new_branch]))
    finally:
        ct.clear()


def test_add_dcline():
    new_dcline = [
        {'capacity': 2000, 'from_bus_id': 200, 'to_bus_id': 2000},
        {'capacity': 1000, 'from_bus_id': 3001001, 'to_bus_id': 1},
        {'capacity': 8000, 'from_bus_id': 12000, 'to_bus_id': 5996}]
    ct.add_dcline(new_dcline)
    new_grid = TransformGrid(grid, ct.ct).get_grid()

    new_pmin = new_grid.dcline.Pmin.values
    new_pmax = new_grid.dcline.Pmax.values

    try:
        assert new_grid.dcline.shape[0] != grid.dcline.shape[0]
        assert np.array_equal(new_pmin[-len(new_dcline):],
                              np.array([-1 * dc['capacity']
                                        for dc in new_dcline]))
        assert np.array_equal(new_pmax[-len(new_dcline):],
                              np.array([dc['capacity'] for dc in new_dcline]))
    finally:
        ct.clear()


def test_add_gen_add_entries_in_plant_data_frame():
    new_plant = [
        {'type': 'solar', 'bus_id': 2050363, 'Pmax': 85},
        {'type': 'wind', 'bus_id': 9, 'Pmin': 5, 'Pmax': 60},
        {'type': 'wind_offshore', 'bus_id': 13802, 'Pmax': 175},
        {'type': 'ng', 'bus_id': 2010687, 'Pmin': 25, 'Pmax': 400,
         'c0': 1500, 'c1': 50, 'c2': 0.5}]
    ct.add_plant(new_plant)
    new_grid = TransformGrid(grid, ct.ct).get_grid()

    new_pmin = new_grid.plant.Pmin.values
    new_pmax = new_grid.plant.Pmax.values
    new_status = new_grid.plant.status.values

    try:
        assert new_grid.plant.shape[0] != grid.plant.shape[0]
        assert np.array_equal(new_pmin[-len(new_plant):],
                              np.array([p['Pmin'] if 'Pmin' in p.keys() else 0
                                        for p in new_plant]))
        assert np.array_equal(new_pmax[-len(new_plant):],
                              np.array([p['Pmax'] for p in new_plant]))
        assert np.array_equal(new_status[-len(new_plant):],
                              np.array([1] * len(new_plant)))
    finally:
        ct.clear()


def test_add_gen_add_entries_in_gencost_data_frame():
    new_plant = [
        {'type': 'solar', 'bus_id': 2050363, 'Pmax': 15},
        {'type': 'wind', 'bus_id': 555, 'Pmin': 5, 'Pmax': 60},
        {'type': 'wind_offshore', 'bus_id': 60123, 'Pmax': 175},
        {'type': 'ng', 'bus_id': 2010687, 'Pmin': 25, 'Pmax': 400,
         'c0': 1500, 'c1': 50, 'c2': 0.5}]
    ct.add_plant(new_plant)
    new_grid = TransformGrid(grid, ct.ct).get_grid()

    new_c0 = new_grid.gencost['before'].c0.values
    new_c1 = new_grid.gencost['before'].c1.values
    new_c2 = new_grid.gencost['before'].c2.values
    new_type = new_grid.gencost['before'].type.values
    new_startup = new_grid.gencost['before'].startup.values
    new_shutdown = new_grid.gencost['before'].shutdown.values
    new_n = new_grid.gencost['before'].n.values

    try:
        assert new_grid.gencost['before'] is new_grid.gencost['after']
        assert new_grid.gencost['before'].shape[0] != grid.gencost[
            'before'].shape[0]
        assert np.array_equal(new_c0[-len(new_plant):],
                              np.array([p['c0'] if 'c0' in p.keys() else 0
                                        for p in new_plant]))
        assert np.array_equal(new_c1[-len(new_plant):],
                              np.array([p['c1'] if 'c1' in p.keys() else 0
                                        for p in new_plant]))
        assert np.array_equal(new_c2[-len(new_plant):],
                              np.array([p['c2'] if 'c2' in p.keys() else 0
                                        for p in new_plant]))
        assert np.array_equal(new_type[-len(new_plant):],
                              np.array([2] * len(new_plant)))
        assert np.array_equal(new_startup[-len(new_plant):],
                              np.array([0] * len(new_plant)))
        assert np.array_equal(new_shutdown[-len(new_plant):],
                              np.array([0] * len(new_plant)))
        assert np.array_equal(new_n[-len(new_plant):],
                              np.array([3] * len(new_plant)))
    finally:
        ct.clear()


def test_add_storage():
    storage = {
        2021005: 116.0,
        2028827: 82.5,
        2028060: 82.5}
    ct.add_storage_capacity(storage)
    new_grid = TransformGrid(grid, ct.ct).get_grid()

    pmin = new_grid.storage['gen'].Pmin.values
    pmax = new_grid.storage['gen'].Pmax.values

    try:
        assert new_grid.storage['gen'].shape[0] != grid.storage['gen'].shape[0]
        assert np.array_equal(pmin, -1 * np.array(list(storage.values())))
        assert np.array_equal(pmax, np.array(list(storage.values())))
    finally:
        ct.clear()
