from enum import Enum
from ctypes.util import find_library
import ctypes

######################################################################
# ENUM INIT
######################################################################


class SqlHandleEnum(Enum):
    SQL_NULL_HANDLE = 0
    SQL_HANDLE_ENV = 1
    SQL_HANDLE_DBC = 2
    SQL_HANDLE_STMT = 3


class SqlAtrVersionEnum(Enum):
    SQL_ATTR_ODBC_VERSION = 200
    SQL_OV_ODBC2 = 2
    SQL_OV_ODBC3 = 3


class SqlStatusEnum(Enum):
    SQL_SUCCESS = 0
    SQL_SUCCESS_WITH_INFO = 1
    SQL_ERROR = -1
    SQL_INVALID_HANDLE = -2
    NO_DATA = 100


class SqlColTypeEnum(Enum):
    def __new__(cls, colattrval, getdataval=None):
        obj = object.__new__(cls)
        obj._value_ = colattrval
        obj.actual = getdataval if getdataval else colattrval
        return obj
    CHAR = 1
    VARCHAR = 12, -8
    LONGVARCHAR = -1
    WCHAR = -8
    WVARCHAR = -9, 1
    WLONGVARCHAR = -10
    DECIMAL = 3
    NUMERIC = 2
    SMALLINT = 5
    INTEGER = 4, 1
    REAL = 7
    FLOAT = 6
    DOUBLE = 8
    BIT = -7
    TINYINT = -6
    BIGINT = -5
    BINARY = -2
    VARBINARY = -3
    LONGVARBINARY = -4
    DATE = 9
    TYPE_DATE = 91
    TYPE_TIME = 92
    TIMESTAMP = 93
    GUID = -11
    SS_VARIANT = -150
    SS_UDT = -151
    SS_XML = -152
    SS_UTCDATETIME = -153
    SS_TIME_EX = -154


class SqlQueryEnum(Enum):
    SQL_ATTR_QUERY_TIMEOUT = 0
    SQL_ATTR_AUTOCOMMIT = 102
    SQL_ATTR_PARAMSET_SIZE = 22


class EnumLoader:
    """
    Load All Enum values as vars as well.
    """
    enum_classes = []

    for class_name in globals():
        if class_name.endswith('Enum'):
            enum_classes.append(class_name)

    for class_name in enum_classes:
        current_class = globals()[class_name]
        values = current_class.__members__
        for value in values.keys():
            globals()[value] = values[value].value

######################################################################
# SQL Library Wrapper 
######################################################################


class SQL:
    env_handle = None
    con_handle = None
    cur_handle = None

    def __new__(cls, *args, **kwargs):
        """
        \rConstructor - Do not inst class if odbc is not installed.
        \rThis class wraps odbc.
        """
        if not find_library('odbc'):
            raise Exception("Unable to find `odbc` shared library")
        return super(SQL, cls).__new__(cls, *args, **kwargs)

    def __init__(self):
        """
        Get and wrap odbc in class.
        """
        self._masquarade('odbc')

    def __del__(self):
        """
        \rClose Cursor and Connection explicitly when gc is activated
        """
        if self.env_handle:
            self.SQLFreeHandle(SQL_HANDLE_ENV, self.env_handle)
        if self.con_handle:
            self.SQLFreeHandle(SQL_HANDLE_DBC, self.con_handle)

    @staticmethod
    def _include(name):
        """
        \rGet a C shared library as a python object
        """
        getter = find_library
        setter = ctypes.CDLL
        return setter(getter(name))

    def _masquarade(self, name):
        """
        \rMerge C shared library python object into current class
        \rAllows expanding and wrapping shared library with python functions
        """
        included = self._include(name)
        self.__class__ = type(included.__class__.__name__, (self.__class__, included.__class__), {})
        self.__dict__ = included.__dict__

    def _gen_handle_env(self):
        """
        \rAllocate env handle
        """
        pointer = ctypes.c_void_p()
        rc = self.SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, ctypes.byref(pointer))
        if rc:
            raise Exception("Unable to create enviorment handle")
        rc = self.SQLSetEnvAttr(pointer, SQL_ATTR_ODBC_VERSION, SQL_OV_ODBC3, 0)
        if rc:
            raise Exception("Unable to set enviorment handle attrebutes")
        self.env_handle = pointer

    def _gen_handle_con(self):
        """
        \rAllocate connection handle
        """
        pointer = ctypes.c_void_p()
        rc = self.SQLAllocHandle(SQL_HANDLE_DBC, self.env_handle, ctypes.byref(pointer))
        if rc:
            raise Exception("Unable to create connection handle")
        rc = self.SQLSetConnectAttr(pointer, SQL_ATTR_AUTOCOMMIT, False, 0)
        if rc:
            raise Exception("Unable to set connection handle attribnutes")
        self.con_handle = pointer

    def _gen_handle_cur(self):
        """
        \rAllocate statement handle
        """
        pointer = ctypes.c_void_p()
        rc = self.SQLAllocHandle(SQL_HANDLE_STMT, self.con_handle, ctypes.byref(pointer))
        if rc:
            raise Exception("Unable to create cursor")
        self.cur_handle = pointer

    def execute(self, query):
        """
        \rRun an sql query
        """
        c_query = ctypes.c_char_p(query.encode())
        rc = self.SQLPrepare(self.cur_handle, c_query, len(query))
        if rc:
            raise Exception("Unable to parse sql statement")
        self.SQLSetStmtAttr(self.cur_handle, SQL_ATTR_PARAMSET_SIZE, 1, 0)
        pointer = ctypes.c_short()
        self.SQLNumParams(self.cur_handle, ctypes.byref(pointer))
        rc = self.SQLExecute(self.cur_handle)
        if rc:
            raise Exception("Failed to execute query")

    def get_column_type(self, i):
        col_type = ctypes.c_ssize_t(0)
        self.SQLColAttribute(self.cur_handle,
                             i + 1,
                             2,
                             ctypes.byref(ctypes.create_string_buffer(10)),
                             10,
                             ctypes.c_short(),
                             ctypes.byref(col_type))
        return SqlColTypeEnum(col_type.value).actual

    def get_column_size(self, i):
        col_size = ctypes.c_ssize_t(0)
        self.SQLColAttribute(self.cur_handle,
                             i + 1,
                             1003,
                             ctypes.byref(ctypes.create_string_buffer(10)),
                             10,
                             ctypes.c_short(),
                             ctypes.byref(col_size))
        return col_size.value

    def get_column_name(self, i):
        col_name = ctypes.create_string_buffer(1024)
        self.SQLColAttribute(self.cur_handle,
                             i + 1,
                             1011,
                             ctypes.byref(col_name),
                             1024,
                             ctypes.c_short(),
                             ctypes.create_string_buffer(1024))
        return col_name.value.decode()

    def check_columns(self):
        columns = []
        for i in range(self.get_results_column_number()):
            column_name = self.get_column_name(i)
            column_type = self.get_column_type(i)
            column_size = self.get_column_size(i)
            columns.append({'name': column_name, 'type': column_type, 'size': column_size})
        return columns

    def fetch(self):
        columns = self.check_columns()
        rows = []

        while True:
            for i in range(self.get_results_column_number()):
                column_name = columns[i]['name']
                column_type = columns[i]['type']
                column_size = columns[i]['size']
                rows.append({})
                rows[-1]['column_name'] = column_name
                column_buffer = ctypes.create_string_buffer(column_size)
                indicator = ctypes.c_ssize_t(column_size)
                self.SQLBindCol(self.cur_handle,
                                i+1,
                                column_type,
                                ctypes.byref(column_buffer),
                                column_size,
                                ctypes.byref(indicator))
                ctypes.memset(ctypes.addressof(column_buffer) + indicator.value, 0, 1)
                rows[-1]['value'] = column_buffer
            rc = int(self.SQLFetch(self.cur_handle))
            if rc == SQL_SUCCESS or rc == SQL_SUCCESS_WITH_INFO:
                pass
            elif rc == NO_DATA:
                rows.pop(len(rows)-1)
                break
            else:
                raise Exception("Query Failed {}".format(rc))
        for row in rows:
            print(row['column_name'], row['value'].raw.decode())

    def get_results_column_number(self):
        buf = ctypes.c_short()
        self.SQLNumResultCols(self.cur_handle, ctypes.byref(buf))
        return buf.value

    def connect(self, hostname, username, password, database='master'):
        """
        \rCONNECT TO data source
        """
        if not self.env_handle:
            self._gen_handle_env()
        if not self.con_handle:
            self._gen_handle_con()
        dsn_tempalte = 'Driver={{ODBC Driver 17 for SQL Server}};Server={};Database={};UID={};PWD={};'
        dsn_string = dsn_tempalte.format(hostname, database, username, password)
        c_dsn = ctypes.c_char_p(dsn_string.encode())
        rc = self.SQLDriverConnect(self.con_handle, 0, c_dsn, len(dsn_string), None, 0, None, 0)
        if rc and rc != 1:
            raise Exception("Failed to connect to the database")

    def gen_cursor(self):
        self._gen_handle_cur()


A = SQL()
A.connect('127.0.0.1', 'sa', 'Aa100100')
A.gen_cursor()
A.execute(r"""select 
              customer_id,
              first_name,
              last_name,
              phone,
              email,
              street,
              city,
              state,
              zip_code 
              from sales.customers
           """)
A.fetch()
