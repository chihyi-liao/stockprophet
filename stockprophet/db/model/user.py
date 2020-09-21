import hashlib
from sqlalchemy import Table, Column
from sqlalchemy.orm import mapper
from sqlalchemy.types import Integer, String

from stockprophet.db import metadata


class User(object):
    def __init__(self, name: str, email: str, password: str):
        m = hashlib.sha1()
        m.update(password.encode("utf-8"))
        self.name = name
        self.email = email
        self.password = m.hexdigest()

    def __repr__(self):
        return "<User('%s','%s')>" % (self.name, self.email)


user_table = Table(
    'user', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('name', String(32), nullable=False),
    Column('email', String(254), unique=True, nullable=False),
    Column('password', String(40), nullable=False))

mapper(User, user_table,
       properties={
           'id': user_table.c.id,
           'name': user_table.c.name,
           'email': user_table.c.email,
           'password': user_table.c.password})

User.__table__ = user_table
