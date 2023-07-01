import json
import os

from corva import ScheduledNaturalTimeEvent
from lambda_function import lambda_handler
from service.constants import DATASET_NAME, DATASET_PROVIDER

MOCK_DATASET_PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)), "datasets")

class TestNaturalTime:
    """
    Test Class for Natural time app
    """
    def test_app(self, app_runner, requests_mock):
        """
        Test application
        """
        event = ScheduledNaturalTimeEvent(
            company_id=1, asset_id=1234, schedule_start=1578291000, interval=60
        )
        dataset_file = os.path.join(MOCK_DATASET_PATH, "hydraulics.surge-and-swab.json")
        with open(dataset_file, encoding="utf8") as raw_dataset:
            hydraulics_surge_and_swab = json.load(raw_dataset)
        requests_mock.get("https://data.localhost.ai/api/v1/data/corva/hydraulics.surge-and-swab/", json=hydraulics_surge_and_swab)
        requests_mock.post(f"https://data.localhost.ai/api/v1/data/{DATASET_PROVIDER}/{DATASET_NAME}/")
        output = app_runner(lambda_handler, event=event)
        assert output

    def test_empty_response_from_dataset(self, app_runner, requests_mock):
        """
        Test empty reponse in data application
        """
        event = ScheduledNaturalTimeEvent(
            company_id=1, asset_id=1234, schedule_start=1578291000, interval=60
        )
        requests_mock.get("https://data.localhost.ai/api/v1/data/corva/hydraulics.surge-and-swab/", json=[])
        output = app_runner(lambda_handler, event=event)
        assert not output
