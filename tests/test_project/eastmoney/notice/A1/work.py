#!/usr/bin/env python3
import json
from argparse import ArgumentParser

from scheduled import Scheduler
from scheduled.storage import RedisStorage
from scheduled import get_logger

from work.fetcher import Fetcher
# from work.pipelines.file_storer import FileStorer
from work.pipelines.sqlite_storer import SqliteStorer


def run():
    parser = ArgumentParser(description='Sina KLine Downloader')
    parser.add_argument('config_file', help='config filename')
    args = parser.parse_args()

    with open(args.config_file) as f:
        config = json.load(f)

    logger = get_logger(**config['logger'])
    storage = RedisStorage(config=config['storage'], logger=logger)
    fetcher = Fetcher(config=config['fetcher'], logger=logger)

    pc = config['pipelines']
    sqlite_storer = SqliteStorer(config=pc.get('sqlite_storer'), logger=logger)
    # file_storer = FileStorer(config=pc.get('file_storer'), logger=logger)

    pipelines = [sqlite_storer]
    scheduler = Scheduler(storage=storage,
                          fetcher=fetcher,
                          pipelines=pipelines, 
                          config=config['scheduler'],
                          logger=logger)
    scheduler.run()


if __name__ == "__main__":
    run()
