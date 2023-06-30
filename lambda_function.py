from corva import Api, Cache, Logger, ScheduledNaturalTimeEvent, scheduled


@scheduled
def lambda_handler(event: ScheduledNaturalTimeEvent, api: Api, cache: Cache):
    """Insert your logic here"""
    Logger.info('Hello, World!')
