from src.common.geo import GeoBucketer


def test_geo_bucketer_produces_consistent_grid():
    bucketer = GeoBucketer(cell_size_km=1.0)
    cell_a = bucketer.bucket(36.1699, -115.1398)
    cell_b = bucketer.bucket(36.1701, -115.1402)

    assert cell_a.grid_id == cell_b.grid_id
    assert abs(cell_a.latitude - cell_b.latitude) < 1e-6
    assert abs(cell_a.longitude - cell_b.longitude) < 1e-6

