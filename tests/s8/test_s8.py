import pytest
from fastapi.testclient import TestClient

from bdi_api.app import app

client = TestClient(app)

def test_list_aircraft():
    response = client.get("/api/s8/aircraft/")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    if data:
        assert "icao" in data[0]
        assert "registration" in data[0]
        assert "type" in data[0]
        assert "owner" in data[0]
        assert "manufacturer" in data[0]
        assert "model" in data[0]

def test_aircraft_co2():
    # Varsayım: En az bir uçak varsa ilkini kullan
    aircrafts = client.get("/api/s8/aircraft/").json()
    if not aircrafts:
        pytest.skip("No aircrafts in database.")
    icao = aircrafts[0]["icao"]
    # Gün olarak bugünün tarihi (örnek: 20250423)
    import datetime
    day = datetime.datetime.now().strftime("%Y%m%d")
    response = client.get(f"/api/s8/aircraft/{icao}/co2?day={day}")
    assert response.status_code == 200
    data = response.json()
    assert "icao" in data
    assert "hours_flown" in data
    assert "co2" in data
