#!/usr/bin/env python3
import click
from main import run_scheduler
from main import run_distributer


@click.command()
@click.argument('config_file', nargs=1)
@click.option('--debug', '-d', default=0, type=int, help='debug mode')
@click.option('--tag', default=None, help='program tag(dummpy)')
def run(config_file, debug, tag):
    run_scheduler(config_file, debug)


if __name__ == '__main__':
    run()
