import yaml
import logging


def yaml_config_logging(yaml_file):
    with open(yaml_file) as f:
        dict_conf = yaml.load(f)
    logging.config.dictConfig(dict_conf)


def file_config_logging(cfg_file):
    logging.config.fileConfig(cfg_file)
