#!/usr/bin/env python3
import os
import sys
import time
import click
import importlib
from configparser import ConfigParser
from scheduled.utils import yaml_config_logging
from scheduled.runs import get_publisher, get_worker, run_worker, get_redis_queue
from scheduled.config import default_config
from scheduled.server import app
# add path
sys.path.insert(0, os.getcwd())


_project_config_file = default_config.get('project_config_file', 'scheduled.cfg')
_default_wait_check_interval = default_config.get('wait_check_interval', 3)
_default_worker_config_file_basename = default_config.get('worker_config_file_basename', 'worker.json')
_default_publisher_config_file_basename = default_config.get('publisher_config_file_basename', 'publisher.json')

def get_config():
    config = ConfigParser(allow_no_value=True)
    config.read(_project_config_file)
    return config


def get_config_key(section, key):
    cfg = get_config()
    return cfg[section].get(key)


cfg = get_config()
logging_yaml_file = cfg['settings']['logging_yaml_file']
yaml_config_logging(logging_yaml_file)


# https://click.palletsprojects.com/en/7.x/arguments/
@click.group()
def cli():
    cfg = get_config()
    click.echo('''** scheduled [%s] **''' % cfg['settings']['project'])
    # click.echo('config logging: %s' % logging_yaml_file)


@cli.command('list')
def list():
    cfg = get_config()
    settings = importlib.import_module('.settings', cfg['settings']['project'])
    modules = getattr(settings, 'MODULES')
    for m in modules:
        click.echo(' '*2 + m)


@cli.command('publisher')
@click.argument('name', nargs=1)
@click.option('--config-file', default=None, 
              type=click.Path(exists=True), 
              help='publisher config file')
def publisher(name, config_file):
    click.echo('Running publisher [%s]...' % name)
    cfg = get_config()
    if config_file is None:
        config_file = os.path.join(cfg['settings']['config_dir'], name,
                                   _default_publisher_config_file_basename)
        if not os.path.exists(config_file): 
            click.echo('Error: `%s` not exists' % config_file)
            sys.exit(-1)
        click.echo('[Publisher] Using default config file: %s' % config_file)
        click.echo('[Publisher] Sleep %f seconds to continue ..' % _default_wait_check_interval)
        time.sleep(_default_wait_check_interval)
    else:
        click.echo('[Publisher] Using config file: %s' % config_file)

    project = get_config_key('settings', 'project')
    default_redis_config_file = get_config_key('settings', 'default_redis_config_file')
    publisher = get_publisher(project, name, config_file, default_redis_config_file)
    publisher.run()


def get_worker_config_file(name):
    cfg = get_config()
    config_dir = cfg['settings'].get('config_dir', 'config')
    config_file = os.path.join(config_dir, name, 
                               _default_worker_config_file_basename)
    if not os.path.exists(config_file): 
        click.echo('Error: config file `%s` not exists' % config_file)
        sys.exit(-1)
    return config_file


@cli.command('worker')
@click.argument('name', nargs=1)
@click.option('--config-file', default=None, 
              type=click.Path(exists=True), 
              help='worker config file')
def worker(name, config_file):
    click.echo('Running worker [%s]...' % name)
    config_file = config_file or get_worker_config_file(name)
    click.echo('[Worker] Using config file: %s' % config_file)
    click.echo('[Worker] Sleep %f seconds to continue ..' % _default_wait_check_interval)
    time.sleep(_default_wait_check_interval)

    project = get_config_key('settings', 'project')
    default_redis_config_file = get_config_key('settings', 'default_redis_config_file')
    worker = get_worker(project, name, config_file, default_redis_config_file)
    worker.run()


@cli.command('scheduler')
@click.argument('name', nargs=1)
# @click.option('--stop', default=0, type=int, help='should stop[0/1]')
@click.option('--config-file', default=None, 
              type=click.Path(exists=True), 
              help='worker config file')
@click.option('--num-workers', '-w', default=1, type=int, help='num workers')
def schedule(name, config_file, num_workers):
    click.echo('Running worker [%s]...' % name)
    cfg = get_config()
    if config_file is None:
        config_dir = cfg['settings'].get('config_dir', 'config')
        config_file = os.path.join(config_dir, name, 
                                   _default_worker_config_file_basename)
        if not os.path.exists(config_file): 
            click.echo('Error: config file `%s` not exists' % config_file)
            sys.exit(-1)
        click.echo('[Worker] Using default config file: %s' % config_file)
        click.echo('[Worker] Sleep %f seconds to continue ..' % _default_wait_check_interval)
        time.sleep(_default_wait_check_interval)
    else:
        click.echo('[Worker] Using config file: %s' % config_file)

    project = get_config_key('settings', 'project')
    default_redis_config_file = get_config_key('settings', 'default_redis_config_file')
    run_worker(project, name, config_file, default_redis_config_file,
               num_workers=num_workers)


@cli.command('server')
@click.option('--host', default=None, help='host')
@click.option('--port', default=None, help='port')
def server(host, port):
    cfg = get_config()
    host = host or cfg['settings'].get('server_host') or '127.0.0.1'
    port = port or cfg['settings'].get('server_port') or 8091
    click.echo('Server at http://%s:%d ..' % (host, int(port)))
    app.run(host=host, port=port)


@cli.command('control')
@click.argument('name', nargs=1)
@click.option('--config-file', default=None, 
              type=click.Path(exists=True), 
              help='worker config file')
@click.option('--stop', default=0, type=click.Choice([0, 1]), help='should stop[0/1]')
@click.option('--doing-to-todo', '--d2t', default=0, type=click.Choice([0, 1]),
              help='should doing-to-todo[0/1]')
def control(name, stop, doing_to_todo, config_file):
    config_file = config_file or get_worker_config_file(name)
    redis_config_file = get_config_key('settings', 'default_redis_config_file')
    q = get_redis_queue(config_file, redis_config_file)
    if stop in [0, 1]:
        click.echo('Mark `stop`=%d' % stop)
        q.mark_stop(stop)
    else:
        click.echo('`stop` not marked.') 

    if doing_to_todo == 1:
        click.echo('All `doing` to `todo`')
        q.all_doings_to_todos()


if __name__ == '__main__':
    cli()
