import yaml
import logging
import logging.config


def yaml_config_logging(yaml_file):
    with open(yaml_file, 'r') as f:
        dict_conf = yaml.load(f, Loader=yaml.FullLoader)
    logging.config.dictConfig(dict_conf)


def file_config_logging(cfg_file):
    logging.config.fileConfig(cfg_file)
