import logging


_default_worker_log_name = 'scheduled.worker'
_default_publisher_log_name = 'scheduled.publisher'

worker_log = logging.getLogger(_default_worker_log_name)
publisher_log = logging.getLogger(_default_publisher_log_name)
