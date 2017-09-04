import logging.config
import logging.handlers
import sys
from datetime import datetime

# Setup logging module

cfg = {
    'version': 1,
    'formatters': {
        'default': {
            'format': '%(asctime)s [%(levelname)s] %(threadName)s:%(name)s:%(lineno)d: %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'default',
            'level': 'INFO',
            'stream': sys.stdout
        },
        'file': {
            'class': 'logging.FileHandler',
            'formatter': 'default',
            'level': 'DEBUG',
            'filename': 'logs/' + datetime.strftime(datetime.now(), '%Y%m%d%H%M%S.%f')[:-3] + '.log'
        }
    },
    'root': {
        'handlers': (
            'console',
            'file'
        ),
        'level': 'DEBUG'
    }
}


logging.config.dictConfig(cfg)
logging.getLogger('requests').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
