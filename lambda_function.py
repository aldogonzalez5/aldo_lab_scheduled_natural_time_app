from datetime import datetime
import statistics

from service.constants import DATASET_NAME, DATASET_PROVIDER 
from corva import Api, Cache, Logger, ScheduledNaturalTimeEvent, scheduled



@scheduled
def lambda_handler(event: ScheduledNaturalTimeEvent, api: Api, cache: Cache):
# 3. Here is where you can declare your variables from the argument event: ScheduledNaturalTimeEvent and start using Api, Cache and Logger functionalities. 

    # The scheduled app can declare the following attributes from the ScheduledNaturalTimeEvent: company_id: The company identifier; asset_id: The asset identifier; schedule_start: The start time of interval; interval: The time between two schedule triggers.
    asset_id = event.asset_id
    company_id = event.company_id
    schedule_start = event.schedule_start
    interval = event.interval

    schedule_end =  schedule_start + interval

# 4. Utilize the attributes from the ScheduledNaturalTimeEvent to make an API request to corva#hydraulics.surge-and-swab or any desired time type dataset.
    # You have to fetch the realtime drilling data for the asset based on start and end time of the event.
    # schedule_start is inclusive so the query is structured accordingly to avoid processing duplicate data
    # We are only querying for mud_density field since that is the only field we need. It is nested under data. We are using the SDK convenience method api.get_dataset. See API Section for more information on convenience method. 
    records = api.get_dataset(
        provider="corva",
        dataset= "hydraulics.surge-and-swab",
        query={
            'asset_id': asset_id,
            'timestamp': {
                '$gte': schedule_start,
                '$lte': schedule_end
            }
        },
        sort={'timestamp': 1},
        limit=500,
        fields="data.mud_density, timestamp"
    )

    record_count = len(records)

    # Utilize the Logger functionality. The default log level is Logger.info. To use a different log level, the log level must be specified in the manifest.json file in the "settings.environment": {"LOG_LEVEL": "DEBUG"}. See the Logger documentation for more information.
    Logger.debug(f"{record_count=}")
    Logger.debug(f"{asset_id=} {company_id=}")
    Logger.debug(f"{schedule_start=} {schedule_end=} {record_count=}")

    if not records:
        readable_date = lambda ts: str(datetime.fromtimestamp(ts))
        Logger.debug("No records to process in current interval")
        Logger.debug(f"schedule start: {readable_date(schedule_start)} schedule end: {readable_date(schedule_end)}")
        return {}
# 5. Implementing some calculations
    # Computing mean mud_density value from the list of realtime wits records
    mean_mud_density = statistics.mean(record.get("data", {}).get("mud_density", 0) for record in records)

    # Utililize the Cache functionality to get a set key value. The Cache functionality is built on Redis Cache. See the Cache documentation for more information.
    # Getting last exported timestamp from Cache 
    last_exported_timestamp = int(cache.get(key='last_exported_timestamp') or 0)

    # Making sure we are not processing duplicate data
    if schedule_end <= last_exported_timestamp:
        Logger.debug(f"Already processed data until {last_exported_timestamp=}")
        return None

# 6. This is how to set up a body of a POST request to store the mean mud_density data and the schedule_start and schedule_end of the interval from the event.
    output = {
        "timestamp": records[-1].get("timestamp"),
        "asset_id": asset_id,
        "company_id": company_id,
        "provider": DATASET_PROVIDER,
        "collection": DATASET_NAME,
        "data": {
            "mean_mud_density": mean_mud_density,
            "schedule_start": schedule_start,
            "schedule_end": schedule_end
        },
        "version": 1
    }

    # Utilize the Logger functionality.

    Logger.debug(f"{output=}")

# 7. Save the newly calculated data in a custom dataset

    # Utilize the Api functionality. The data=outputs needs to be an an array because Corva's data is saved as an array of objects. Objects being records. See the Api documentation for more information.
    api.post(
        f"api/v1/data/{DATASET_PROVIDER}/{DATASET_NAME}/", data=[output],
    ).raise_for_status()

    # Utililize the Cache functionality to set a key value. The Cache functionality is built on Redis Cache. See the Cache documentation for more information. This example is setting the last timestamp of the output to Cache
    cache.set(key='last_exported_timestamp', value=records[-1].get("timestamp"))

    return output