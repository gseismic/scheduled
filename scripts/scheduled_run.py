#!/usr/bin/env python3
import os
import sys
import time
import click
import logging
import importlib
from configparser import ConfigParser
from scheduled.utils import yaml_config_logging
from scheduled.runs import get_distributer, get_worker

# add path
sys.path.append(os.getcwd())


DEFAULT_CHECK_INTERVAL = 5
DEFAULT_WORKER_CONFIG_FILE = 'worker.json'
DEFAULT_DISTRIBUTER_CONFIG_FILE = 'distributer.json'


# from main.runs import run_distributer
def get_config():
    config = ConfigParser(allow_no_value=True)
    config.read('scheduled.cfg')
    return config


def get_config_key(section, key):
    cfg = get_config()
    return cfg[section].get(key)


# https://click.palletsprojects.com/en/7.x/arguments/
@click.group()
def cli():
    cfg = get_config()
    click.echo('''** schedules [%s] **''' % cfg['settings']['project'])
    logging_yaml_file = cfg['settings']['logging_yaml_file']
    click.echo('config logging: %s' % logging_yaml_file)
    yaml_config_logging(logging_yaml_file)


@cli.command('distributer')
@click.argument('name', nargs=1)
@click.option('--config-file', default=None, 
              type=click.Path(exists=True), 
              help='distributer config file')
def distributer(name, config_file):
    click.echo('Running distributer [%s]...' % name)
    cfg = get_config()
    if config_file is None:
        config_file = os.path.join(cfg['settings']['config_dir'], 
                                   name, DEFAULT_DISTRIBUTER_CONFIG_FILE)
        if not os.path.exists(config_file): 
            print('Error: `%s` not exists' % config_file)
            sys.exit(-1)
        click.echo('[Distributer] Using default config file: %s' % config_file)
        click.echo('[Distributer] Sleep %f seconds to continue ..' % DEFAULT_CHECK_INTERVAL)
        time.sleep(DEFAULT_CHECK_INTERVAL)
    else:
        click.echo('[Distributer] Using config file: %s' % config_file)

    project = get_config_key('settings', 'project')
    default_redis_config_file = get_config_key('settings', 'default_redis_config_file')
    distributer = get_distributer(project, name, config_file, default_redis_config_file)
    distributer.run()


@cli.command('list')
def list_():
    cfg = get_config()
    settings = importlib.import_module('.settings', cfg['settings']['project'])
    modules = getattr(settings, 'MODULES')
    for m in modules:
        click.echo('\t' + m)


@cli.command('worker')
@click.argument('name', nargs=1)
@click.option('--config-file', default=None, 
              type=click.Path(exists=True), 
              help='worker config file')
def worker(name, config_file):
    click.echo('Running worker [%s]...' % name)

    cfg = get_config()
    if config_file is None:
        config_file = os.path.join(cfg['settings']['config_dir'], name, 
                                   DEFAULT_WORKER_CONFIG_FILE)
        if not os.path.exists(config_file): 
            print('Error: `%s` not exists' % config_file)
            sys.exit(-1)
        click.echo('[Worker] Using default config file: %s' % config_file)
        click.echo('[Worker] Sleep %f seconds to continue ..' % DEFAULT_CHECK_INTERVAL)
        time.sleep(DEFAULT_CHECK_INTERVAL)
    else:
        click.echo('[Worker] Using config file: %s' % config_file)

    project = get_config_key('settings', 'project')
    default_redis_config_file = get_config_key('settings', 'default_redis_config_file')
    worker = get_worker(project, name, config_file, default_redis_config_file)
    worker.run()


if __name__ == '__main__':
    cli()
