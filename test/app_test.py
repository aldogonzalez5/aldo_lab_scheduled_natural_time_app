from corva import ScheduledNaturalTimeEvent
from lambda_function import lambda_handler


def test_app(app_runner):
    event = ScheduledNaturalTimeEvent(
        company_id=1, asset_id=1234, schedule_start=1578291000, interval=60
    )

    app_runner(lambda_handler, event=event)
