from sqlalchemy import create_engine
from flask import current_app, g
import platform

import config

"""
def get_dyna_db():
    if 'dyna_db' not in g:
        dyna_server = current_app.config['dynaServer']
        dyna_db = current_app.config['dynaDBName']
        dyna_user = current_app.config['dynaUserName']
        dyna_password = current_app.config['dynaPassword']
        dynacom_eng = \
            create_engine(f'mssql+pyodbc://{dyna_user}:{dyna_password}@{dyna_server}/'
                          f'{dyna_db}?driver=SQL Server')
        g.dyna_db = dynacom_eng.connect()
    return g.dyna_db


def get_db():
    if 'db' not in g:
        postgres_server = current_app.config['postgresServer']
        postgres_db = current_app.config['postgresDBName']
        postgres_user = current_app.config['postgresUserName']
        postgres_password = current_app.config['postgresPassword']
        postgres_eng = \
            create_engine(f'postgresql://{postgres_user}:{postgres_password}@'
                          f'{postgres_server}:5432/{postgres_db}')
        g.db = postgres_eng.connect()
    return g.db


def close_dyna_db(e=None):
    db = g.pop('dyna_db',None)

    if db is not None:
        db.close()


def close_db(e=None):
    db = g.pop('db', None)

    if db is not None:
        db.close()
"""


def get_dyna_db():
    """
    Create Dynacom DB engine outside the app context, for worker and
    scheduled tasks. Returns sqlalchemy engine
    """
    dyna_server = config.dynaServer
    dyna_db = config.dynaDBName
    dyna_user = config.dynaUserName
    dyna_password = config.dynaPassword
    dynacom_conn = f'mssql+pyodbc://{dyna_user}:{dyna_password}@{dyna_server}/{dyna_db}'
    if platform.system() == 'Windows':
        dynacom_driver = 'SQL Server'
    else:
        dynacom_driver = 'ODBC+Driver+17+for+SQL+Server'
    dynacom_eng = create_engine(dynacom_conn+'?driver='+dynacom_driver)
    return dynacom_eng


def get_db():
    """
    Create PostgreSQL DB engine outside the app context, for worker and
    scheduled tasks. Returns sqlalchemy engine
    """
    postgres_server = config.postgresServer
    postgres_db = config.postgresDBName
    postgres_user = config.postgresUserName
    postgres_password = config.postgresPassword
    postgres_eng = \
        create_engine(f'postgresql://{postgres_user}:{postgres_password}@'
                      f'{postgres_server}:5432/{postgres_db}')

    return postgres_eng

"""
def init_app(app):
    app.teardown_appcontext(close_db)
    app.teardown_appcontext(close_dyna_db)
"""
