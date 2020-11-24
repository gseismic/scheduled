import json
from .scheduled import Distributer, get_logger
from .scheduled.storage import RedisStorage
from .key_yielder import KeyYielder


def run_distributer(config_file):
    with open(config_file) as f:
        config = json.load(f)

    logger = get_logger(**config['logger'])
    key_yielder = KeyYielder(config=config['key_yielder'], logger=logger)
    storage = RedisStorage(config=config['storage'], logger=logger)
    distributer = Distributer(storage=storage, key_yielder=key_yielder,
                              config=config['distributer'], logger=logger)
    distributer.run()
