import os
import time
import json
import importlib
from .queue import RedisQueue
from .publisher import Publisher
from .worker import Worker
from .logger import publisher_log, worker_log, root_logger
from .module_utils import classname_to_configname
from .module_utils import join_module_path, split_moduleclass_path, get_module_and_attr 


def get_submodule_settings(project, name):
    submodule_settings = importlib.import_module(join_module_path(project, name, 'settings'))
    return submodule_settings


def get_jsonfile_key(config_file, key):
    with open(config_file) as f:
        config = json.load(f)
    return config.get(key)


def get_class(project, name, key):
    # setting
    submodule_settings = get_submodule_settings(project, name)
    # key_yielder.KeyYielder
    values = getattr(submodule_settings, key) 

    if isinstance(values, (tuple, list)):
        cls_list = []
        # PIPELINES
        for value in values:
            full_path = join_module_path(project, name, value) 
            # str: project.name.key_yielder, KeyYielder
            module_name, class_name = split_moduleclass_path(full_path)
            # object: project.name.key_yielder, KeyYielder
            module, cls = get_module_and_attr(module_name, class_name)
            cls_list.append(cls)
        return cls_list
    else:
        # project.name.key_yielder.KeyYielder
        full_path = join_module_path(project, name, values) 
        # str: project.name.key_yielder, KeyYielder
        module_name, class_name = split_moduleclass_path(full_path)
        # object: project.name.key_yielder, KeyYielder
        module, cls = get_module_and_attr(module_name, class_name)
        return cls


def get_redis_queue(config, default_redis_config_file):
    if isinstance(config, str):
        with open(config) as f:
            config = json.load(f)
    queue_id = config['redis_queue'].get('queue_id')
    redis_uri = config['redis_queue'].get('redis_uri')
    if not redis_uri:
        redis_uri = get_jsonfile_key(default_redis_config_file, 'redis_uri')
    assert(redis_uri)
    queue = RedisQueue(queue_id, redis_uri)
    return queue


def get_publisher(project, name, config_file, default_redis_config_file):
    if not os.path.exists(config_file):
        raise Exception('Config file not exists: %s' % config_file)

    with open(config_file) as f:
        config = json.load(f)

    logger = publisher_log
    yielder_cls = get_class(project, name, 'KEY_YIELDER')
    key_yielder = yielder_cls(config=config['key_yielder'], logger=logger)
    queue = get_redis_queue(config, default_redis_config_file)
    publisher = Publisher(queue=queue, key_yielder=key_yielder,
                          config=config['publisher'], logger=logger)
    return publisher


def get_pipelines(project, name, pipe_config, logger):
    pipelines = []
    submodule_settings = get_submodule_settings(project, name)
    values = getattr(submodule_settings, 'PIPELINES')
    for value in values:
        # str: project.name.pipelines.parser.Parser
        full_path = join_module_path(project, name, value) 
        # str: (project.name.pipelines.parser, Parser)
        module_name, class_name = split_moduleclass_path(full_path)
        module, pipe_cls = get_module_and_attr(module_name, class_name)

        config_key = classname_to_configname(class_name)
        config = pipe_config[config_key]

        pipe = pipe_cls(config=config, logger=logger)
        pipelines.append(pipe)
    return pipelines


def get_fetcher(project, name, config, logger):
    fetcher_cls = get_class(project, name, 'FETCHER')
    fetcher = fetcher_cls(config=config['fetcher'], logger=logger)
    return fetcher


def get_worker(project, name, config_file, default_redis_config_file):
    if not os.path.exists(config_file):
        raise Exception('Config file not exists: %s' % config_file)

    with open(config_file) as f:
        config = json.load(f)

    logger = worker_log
    fetcher = get_fetcher(project, name, config, logger)
    pipelines = get_pipelines(project, name, config['pipelines'], logger)
    queue = get_redis_queue(config, default_redis_config_file)

    worker = Worker(queue=queue,
                    fetcher=fetcher,
                    pipelines=pipelines,
                    config=config['worker'], 
                    logger=logger)
    return worker


def task_worker_run(worker):
    pass
    

# 为signal
from scheduled.config import default_config
_project_config_file = default_config.get('project_config_file', 'scheduled.cfg')
def get_config():
    config = ConfigParser(allow_no_value=True)
    config.read(_project_config_file)
    return config

def get_config_key(section, key):
    cfg = get_config()
    return cfg[section].get(key)

def get_worker_config_file(name):
    cfg = get_config()
    config_dir = cfg['settings'].get('config_dir', 'config')
    config_file = os.path.join(config_dir, name, 
                               _default_worker_config_file_basename)
    if not os.path.exists(config_file): 
        click.echo('Error: config file `%s` not exists' % config_file)
        sys.exit(-1)
    return config_file


def run_worker(project, name, config_file, 
               default_redis_config_file,
               num_workers):
    assert(num_workers >= 1)
    from multiprocessing import Process

    def manage_sigint(*args, **kwargs):
        config_file = config_file or get_worker_config_file(name)
        redis_config_file = get_config_key('settings', 'default_redis_config_file')
        q = get_redis_queue(config_file, redis_config_file)
        q.mark_stop(1)

    import signal
    signal.signal(signal.SIGINT, manage_sigint)

    processes = []
    for i in range(num_workers):
        worker = get_worker(project, name, config_file, default_redis_config_file)
        process = Process(target=worker.run)
        processes.append(process)

    for p in processes:
        p.start()
        time.sleep(1)
        root_logger.info('Process %s started' % str(p.pid))

    for p in processes:
        p.join()
    # import atexit
    # root_logger.info('Done!')


