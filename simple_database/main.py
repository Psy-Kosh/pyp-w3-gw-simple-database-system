# -*- coding: utf-8 -*-
import os
from datetime import date

from simple_database.exceptions import ValidationError
from simple_database.config import BASE_DB_FILE_PATH
import json

class RowObject(object):
    def __init__(self, row):
        self._row = row
    def __getattr__(self, name):
        return self._row[name]

class Table(object):

    def __init__(self, db, name, table_dict):
        self.db = db
        self.name = name
        self._table_dict = table_dict
        self._columns = table_dict['columns']
        self._data = table_dict['data']
        
    def __getattr__(self, name):
        return self._data[name]

    def insert(self, *args):
        if len(args) != len(self._columns):
            raise ValidationError('Invalid amount of field')

        row = {}
        for argument, column in zip(args, self._columns):
            argument_type = type(argument).__name__
            column_type, column_name = column['type'], column['name']
            if argument_type != column_type:
                raise ValidationError('Invalid type of field "{}": Given "{}", expected "{}"'.format(column_name, argument_type, column_type))
            row[column_name] = argument
            
        self._data.append(row)
        self.db._save_db()

    def query(self, **kwargs):
        for row in self._data:
            if all(row[name] == value for name, value in kwargs.items()):
                yield RowObject(row)

    def all(self):
        return (RowObject(row) for row in self._data)

    def count(self):
        return len(self._data)

    def describe(self):
        return self._columns

class DataBase(object):
    @staticmethod
    def db_file(name):
        return '{}{}.json'.format(BASE_DB_FILE_PATH, name)
        
    def _save_db(self):
        with open(self.db_file(self.name), 'w') as file:
            json.dump(self._db_dict, file, default = str)
    
    def __init__(self, name):
        self.name = name
        with open(self.db_file(name)) as file:
            self._db_dict = json.load(file)
        
    def __getattr__(self, name):
        return Table(self, name, self._db_dict[name])

    @classmethod
    def create(cls, name):
        try: os.makedirs(BASE_DB_FILE_PATH)
        except OSError as exc:
            if exc.errno != os.errno.EEXIST:
                raise
        filename = cls.db_file(name)
        if os.path.isfile(filename):
            raise ValidationError('Database with name "{}" already exists.'.format(name))
        with open(filename, 'w') as file:
            json.dump({}, file)

    def create_table(self, table_name, columns):
        if table_name in self._db_dict:
            raise ValidationError('Table with name {} already exists.'.format(table_name))
        self._db_dict[table_name] = {'columns':columns, 'data':[]}
        self._save_db()

    def show_tables(self):
        return [name for name in self._db_dict]


def create_database(db_name):
    """
    Creates a new DataBase object and returns the connection object
    to the brand new database.
    """
    DataBase.create(db_name)
    return connect_database(db_name)


def connect_database(db_name):
    """
    Connectes to an existing database, and returns the connection object.
    """
    return DataBase(name=db_name)
