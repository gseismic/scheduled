import os
import json
import logging
import importlib
from .queue import RedisQueue
from .distributer import Distributer
from .worker import Worker
from .module_utils import join_module_path, split_moduleclass_path, get_module_and_attr 
from .module_utils import classname_to_configname


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
    queue_id = config['redis_queue'].get('queue_id')
    redis_uri = config['redis_queue'].get('redis_uri')
    if not redis_uri:
        redis_uri = get_jsonfile_key(default_redis_config_file, 'redis_uri')
    assert(redis_uri)
    queue = RedisQueue(queue_id, redis_uri)
    return queue


def get_distributer(project, name, config_file, default_redis_config_file):
    if not os.path.exists(config_file):
        raise Exception('Config file not exists: %s' % config_file)

    with open(config_file) as f:
        config = json.load(f)

    logger = logging.getLogger('distributer')
    yielder_cls = get_class(project, name, 'KEY_YIELDER')
    key_yielder = yielder_cls(config=config['key_yielder'], logger=logger)
    queue = get_redis_queue(config, default_redis_config_file)
    distributer = Distributer(queue=queue, key_yielder=key_yielder,
                              config=config['distributer'], logger=logger)
    return distributer


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

    logger = logging.getLogger('worker')
    fetcher = get_fetcher(project, name, config, logger)
    pipelines = get_pipelines(project, name, config['pipelines'], logger)
    queue = get_redis_queue(config, default_redis_config_file)

    worker = Worker(queue=queue,
                    fetcher=fetcher,
                    pipelines=pipelines,
                    config=config['worker'], 
                    logger=logger)
    return worker
