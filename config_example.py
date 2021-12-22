dynaServer = '<insert server address here>'
dynaDBName = '<insert db name here>'
dynaUserName = '<insert username here>'
dynaPassword = '<insert password here>'
mode = 'development'  # change to 'production' for production servers

# Default log config
log_config = {
    'version': 1,
    'formatters': {'default': {
        'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
    }},
    'handlers': {'wsgi': {
        'class': 'logging.StreamHandler',
        'stream': 'ext://flask.logging.wsgi_errors_stream',
        'formatter': 'default'
    },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'formatter': 'default',
            'filename': 'apilogs.log',
            'maxBytes': 512000,
            'backupCount': 3
        }
    },
    'root': {
        'level': 'INFO',
        'handlers': ['wsgi', 'file']
    }
}
