import os
from datetime import date
import json

from simple_database.exceptions import ValidationError
from simple_database.config import BASE_DB_FILE_PATH


class Row(object):
    def __init__(self, row):
        self.row = row
        
        for key, value in row.items():
            setattr(self, key, value)


class Table(object):

    def __init__(self, db, name, columns=None):
        self.db = db
        self.name = name
        self.table_filepath = os.path.join(BASE_DB_FILE_PATH, self.db.name,
                                           '{}.json'.format(self.name))

        # In case the table JSON file doesn't exist already, you must
        # initialize it as an empty table, with this JSON structure:
        # {'columns': columns, 'rows': []}
        
        self.columns = columns or self._read_columns()
        
        if not os.path.exists(self.table_filepath):
            with open(self.table_filepath, 'w') as table:
                table_format = {'columns': columns, 'rows': []}
                json.dump(table_format, table)
        
        
    def _read_columns(self):
        # Read the columns configuration from the table's JSON file
        # and return it.
        
        with open(self.table_filepath, 'r') as table:
            data = json.load(table)
            return data['columns']
        
        
    def insert(self, *args):
        # Validate that the provided row data is correct according to the
        # columns configuration.
        # If there's any error, raise ValidationError exception.
        # Otherwise, serialize the row as a string, and write to to the
        # table's JSON file.
        if len(args) != len(self.columns):
            raise ValidationError('Invalid amount of fields')
        for index, dic in enumerate(self.columns):
            if type(args[index]) is not eval(dic['type']):                                                     
                raise ValidationError('Invalid type of field "{}": Given "{}", expected "{}"'.format(dic['name'], type(args[index]).__name__, dic['type']))
                
        row_dict = {dic['name']: arg for dic, arg in zip(self.columns, args)}
        for key in row_dict.keys():
            if isinstance(row_dict[key], date):
                row_dict[key] = row_dict[key].isoformat()
                
        # write row_dict to file here
        with open(self.table_filepath, 'r+') as table:
            data = json.load(table)
            data['rows'].append(row_dict)
            table.seek(0)
            json.dump(data, table)


    def query(self, **kwargs):
    # Read from the table's JSON file all the rows in the current table
    # and return only the ones that match with provided arguments.
    # We would recomment to  use the `yield` statement, so the resulting
    # iterable object is a generator.

    # IMPORTANT: Each of the rows returned in each loop of the generator
    # must be an instance of the `Row` class, which contains all columns
    # as attributes of the object.
        with open(self.table_filepath, 'r') as table:
            data = json.load(table)
            for item in data['rows']:
                if not all([item[key]==value for key,value in kwargs.items()]):
                    continue
                yield Row(item)
     
    
    def all(self):
        # Similar to the `query` method, but simply returning all rows in
        # the table.
        # Again, each element must be an instance of the `Row` class, with
        # the proper dynamic attributes.
        
        with open(self.table_filepath, 'r') as table:
            data = json.load(table)
            for item in data['rows']:
               yield Row(item)
                

    def count(self):
        # import pdb
        # pdb.set_trace()
        with open(self.table_filepath, 'r') as table:
            data = json.load(table)
            return len(data['rows'])
        # Read the JSON file and return the counter of rows in the table

    def describe(self):
        return self.columns
        # Read the columns configuration from the JSON file, and return it.


class DataBase(object):
    def __init__(self, name):
        self.name = name
        self.db_filepath = os.path.join(BASE_DB_FILE_PATH, self.name)
        self.tables = self._read_tables()

    @classmethod
    def create(cls, name):
        db_filepath = os.path.join(BASE_DB_FILE_PATH, name)
        if os.path.exists(db_filepath):
            # if the db directory already exists, raise ValidationError
            # otherwise, create the proper db directory
            raise ValidationError('Database with name "{}" already exists.'.format(name))
        os.makedirs(db_filepath)
        return DataBase(name)

    def _read_tables(self):
        # Gather the list of tables in the db directory looking for all files
        # with .json extension.
        # For each of them, instatiate an object of the class `Table` and
        # dynamically assign it to the current `DataBase` object.
        # Finally return the list of table names.
        # Hint: You can use `os.listdir(self.db_filepath)` to loop through
        #       all files in the db directory
        names = []
        for file in os.listdir(self.db_filepath):
            # Split filename and check extension
            ext = os.path.splitext(file)[-1].lower()
            filename = os.path.splitext(file)[0].lower()
            if ext == '.json':
                # assign to current database
                setattr(self, filename, Table(self, filename)) # add columns?
                names.append(filename)
        return names

    def create_table(self, table_name, columns):
        # Check if a table already exists with given name. If so, raise
        # ValidationError exception.
        # Otherwise, create an instance of the `Table` class and assign
        # it to the current db object.
        # Make sure to also append it to `self.tables`
        if hasattr(self, table_name):
            raise ValidationError('Table with name "{}" already exists.'.format(table_name))
        new_table = Table(self, table_name, columns)
        setattr(self, table_name, new_table)
        self.tables.append(table_name)

    def show_tables(self):
        # Return the current list of tables.
        return self.tables


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
