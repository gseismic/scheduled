#!/usr/bin/env python3
import click
from main import run_distributer


@click.command()
@click.argument('config_file', nargs=1)
def run(config_file):
    run_distributer(config_file)


if __name__ == '__main__':
    run()
