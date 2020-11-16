import threading

from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import scoped_session, sessionmaker


_ENGINE = None
_SESSION = None

metadata = MetaData()
db_lock = threading.Lock()


def init_db(config: dict):
    global _ENGINE, _SESSION
    if _ENGINE is not None:
        _ENGINE = None

    if _SESSION is not None:
        _SESSION.close()

    drivername = config.get('drivername', '')
    if 'sqlite' in drivername:
        _ENGINE = create_engine(URL(**config), connect_args={"check_same_thread": False})
    else:
        _ENGINE = create_engine(URL(**config), pool_recycle=3600)

    _SESSION = scoped_session(sessionmaker(
        autocommit=False, autoflush=True, expire_on_commit=False, bind=_ENGINE))
    metadata.create_all(bind=_ENGINE)


def recreate_session():
    global _SESSION, _ENGINE
    try:
        if _SESSION is not None:
            _SESSION.close()
    except Exception:
        raise
    _SESSION = None
    _SESSION = scoped_session(sessionmaker(autocommit=False,
                                           autoflush=True,
                                           expire_on_commit=False,
                                           bind=_ENGINE))


def get_local_session():
    global _ENGINE
    session = sessionmaker(
        autocommit=False, autoflush=True,
        expire_on_commit=False, bind=_ENGINE)()
    return session


def get_session():
    global _SESSION
    return _SESSION


def get_engine():
    global _ENGINE
    return _ENGINE
