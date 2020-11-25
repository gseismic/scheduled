import logging


_default_root_log_name = 'root'
_default_worker_log_name = 'worker'
_default_publisher_log_name = 'publisher'

root_logger = logging.getLogger(_default_root_log_name)
worker_log = logging.getLogger(_default_worker_log_name)
publisher_log = logging.getLogger(_default_publisher_log_name)
