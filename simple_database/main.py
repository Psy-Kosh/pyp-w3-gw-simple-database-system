# -*- coding: utf-8 -*-
import os
from datetime import date

from simple_database.exceptions import ValidationError
from simple_database.config import BASE_DB_FILE_PATH
import json

# BASE_DB_FILE_PATH = '/tmp/simple_database/'

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
        # self.db.create_table('authors', columns=[
        # {'name': 'id', 'type': 'int'}], etc
        # open json, assign it to variable called json_object. write to it.
        # json_object[name] = {} # create empty table (eg: json_object['authors'] = {})
        # json_object['columns'] = columns # set the columns
        # json_object['data'] = {}
        # need to save this somehow. do we return the json object?
        
    def __getattr__(self, name):
        return self._data[name]

    def insert(self, *args):
        # open json, assign it to variable called table
        # get name of table, assign it to table_name

        #number_of_columns = len(table[table_name]['columns'])
        # if len(args) != number_of_columns:
        if len(args) != len(self._columns):
            raise ValidationError('Invalid amount of field')
        
        # find the line under data that has one less comma, that is the end of the current data
 
        # for arg in args:
        #     if type(arg) != columns_list[0]['type']:
        #     pass
        #check that each argument has the correct type
        row = {}
        for arg, col in zip(args, self._columns):
            argtype = type(arg).__name__
            coltype, colname = col['type'], col['name']
            if argtype != coltype:
                raise ValidationError('Invalid type of field "{}": Given "{}", expected "{}"'.format(colname, argtype, coltype))
            row[colname] = arg
        self._data.append(row)
        self.db._save_db()

    def query(self, **kwargs):
        return (RowObject(row) for row in self._data if all(row[name] == value for name, value in kwargs.items()))
            

    def all(self):
        return (RowObject(row) for row in self._data)

    def count(self):
        # again, open the json file and assign it to a variable called table
        # get the table_name
        # return len(table[table_name]['data'])
        return len(self._data)

    def describe(self):
        # something to open the .json file and assign it to a variable
        # copypasted the code we'll modify it for our purposes
        # weather = urllib2.urlopen('url')
        # json_table = weather.read()
        # table = json.loads(json_table)
        
        # need to get the table_name like authors
        # return table[table_name]['columns']
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
