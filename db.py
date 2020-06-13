from collections.abc import MutableMapping

class Database(MutableMapping):
  def __init__(self):
    self.database = {}
  def __getitem__(self,key):
    return self.database.get(key)
  def __setitem__(self,key,value):
    if key not in self.database.keys():
      self.database[key] = Table(value)
  def __delitem__(self,key):
    if self.database.get(key):
      del self.database[key]
  def __iter__(self):
    return iter(self.database)
  def __len__(self):
    return len(self.database)
  def __repr__(self):
    return '{}({})'.format(self.__class__.__name__,list(self.database.keys()))
  def keys(self):
    return list(self.database.keys())
  def create_table(self,name):
    self[name] = name
    return self[name]
  def create_column(self,table,name,type):
    self[table].create_column(name,type)
  def create_columns(self,table,names,types):
    for name,type in zip(names,types):
      self[table].create_column(name,type)
  def get_tables(self):
    return list(self.database)
  def get_columns(self,name):
    return list(self[name])
  def get_rows(self,name):
    return self[name].values()
  def insert(self,name,row):
    self[name].insert(row)
  def update_col(self,table_name,column_name,old,new):
    column = self[table_name][column_name]
    for i in range(len(column)):
      if str(column[i]) == str(old):
        column[i] = new
  def select(self,table_name,col_name,value):
    ret_list = []
    column = self[table_name][col_name]
    for n in range(len(column)):
      if str(value) == str(column[n]):
        ret_list.append([i[n] for i in self[table_name].values() ])
    return ret_list
  def drop_table(self,name):
    self.pop(name)


class Table(MutableMapping):
  def __init__(self,name):
    self.table = {}
    self.name = name
  def __getitem__(self,key):
    return self.table[key]
  def __setitem__(self,key,type):
    if key not in self.table.keys():
        self.table[key] = Column(type,key)
    else:
        raise Exception("Attempting to add an existing column")
  def __delitem__(self,key):
    del self.table[key]
  def __iter__(self):
    return iter(self.table)
  def __len__(self):
    if len(self.table) == 0:
      return 0
    else:
      return len(self.table[list(self.table.keys())[0]])
  def __repr__(self):
     return '{} {}({})'.format(self.__class__.__name__,self.name,list(self.table.keys()))
  def create_column(self,name,type):
    self[name]=type
    return self[name]
  def keys(self):
    return list(self.table.keys())
  def values(self):
    return [ i for i in self.table.values() ]
  def insert(self,row):
    for (col_val,col) in zip(row,self.table.values()):
        col.append(col_val)

class Column(MutableMapping):
  def __init__(self,col_type,col_name):
    self.column = []
    self.col_type = col_type
    self.col_name = col_name
  def __getitem__(self,row):
    return self.column[row]
  def __setitem__(self,row,value):
    length = len(self.column)
    if row == length:
      self.column.append(Value(value,type=self.col_type))
    elif row > length:
      raise Exception("Skipping row order")
    else:
      self.column[row] = Value(value,type=self.col_type)
  def __delitem__(self,row):
    del self.column[row]
  def __iter__(self):
    return iter(self.column)
  def __len__(self):
    return len(self.column)
  def __repr__(self):
    return '{} {}({})'.format(self.__class__.__name__,self.col_name,self.column)
  def append(self,values):
    if isinstance(values,list):
      for value in values:
        self[len(self)] = value
    else:
        self[len(self)] = values

class Value:
  def __new__(cls,*args,**kwargs):
    if 'type' in kwargs.keys():
      try:
        n_args = kwargs['type'](args[0])
      except:
        raise Exception("Data Type does not conforms")
    else:
      raise Exception("No Data Type Provided")
    return n_args


"""
DB = Database()
Dragons = DB.create_table('Dragons')
DB.create_columns('Dragons',['id','Name'],[int,str])
DB.insert('Dragons',[[1,2],['Daniel','Dan']])
print(DB.get_rows('Dragons'))
DB.drop_table('Dragons')
"""
