import json
from .scheduled import Scheduler, get_logger
from .scheduled.storage import RedisStorage
from .fetcher import Fetcher
from .pipelines.file_storer import FileStorer


def run_scheduler(config_file, debug=0):
    with open(config_file) as f:
        config = json.load(f)

    try:
        debug_ = debug is True or debug > 0 
    except:
        debug_ = False

    logger = get_logger(**config['logger'])
    storage = RedisStorage(config=config['storage'], logger=logger)
    fetcher = Fetcher(config=config['fetcher'], logger=logger)

    pc = config['pipelines']
    file_storer = FileStorer(config=pc.get('file_storer'), logger=logger)

    pipelines = [file_storer]
    scheduler = Scheduler(storage=storage,
                          fetcher=fetcher,
                          pipelines=pipelines, 
                          config=config['scheduler'],
                          logger=logger,
                          debug=debug_)
    logger.info('run_scheduler ...')
    scheduler.run()
