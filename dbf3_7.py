#! /usr/bin/env python
"""DBF accessing helpers.

FIXME: more documentation needed

Examples:

    Create new table, setup structure, add records:

        dbf = Dbf(filename, new=True)
        dbf.addField(
            ("NAME", "C", 15),
            ("SURNAME", "C", 25),
            ("INITIALS", "C", 10),
            ("BIRTHDATE", "D"),
        )
        for (n, s, i, b) in (
            ("John", "Miller", "YC", (1980, 10, 11)),
            ("Andy", "Larkin", "", (1980, 4, 11)),
        ):
            rec = dbf.newRecord()
            rec["NAME"] = n
            rec["SURNAME"] = s
            rec["INITIALS"] = i
            rec["BIRTHDATE"] = b
            rec.store()
        dbf.close()

    Open existed dbf, read some data:

        dbf = Dbf(filename, True)
        for rec in dbf:
            for fldName in dbf.fieldNames:
                print '%s:\t %s (%s)' % (fldName, rec[fldName],
                    type(rec[fldName]))
            print
        dbf.close()

"""
"""History (most recent first):
09-Jun-2019 [psc]   1. Upgrade for python3.7
                    2. fix a few bugs
                    3. add the following interfaces to DBF class:
                       GetFieldNameList()
                       GetNumberOfRecords()
                       AppendRecord()
                       CreateFieldsFromFile()
                       AppendRecordsFromFile()
                       printfDbf()
                       printRecord()
                       GetDefinitions()
                       FieldNameExists()
                       FindRecords()
                       FindAndReplaceRecords()
   
                   4.  add the following whole interfaces:     
                       CopyDBF()
                       CreateFieldsFromFile()
                       ReplaceFilesInFileList()
                       AppendRecordsToFile()
                       FindRecordsFromFile()
                       ReplaceFilesInFileList()
                       ReplaceFilesInDirectory()
                    
11-feb-2007 [als]   export INVALID_VALUE;
                    Dbf: added .ignoreErrors, .INVALID_VALUE
04-jul-2006 [als]   added export declaration
20-dec-2005 [yc]    removed fromStream and newDbf methods:
                    use argument of __init__ call must be used instead;
                    added class fields pointing to the header and
                    record classes.
17-dec-2005 [yc]    split to several modules; reimplemented
13-dec-2005 [yc]    adapted to the changes of the `strutil` module.
13-sep-2002 [als]   support FoxPro Timestamp datatype
15-nov-1999 [jjk]   documentation updates, add demo
24-aug-1998 [jjk]   add some encodeValue methods (not tested), other tweaks
08-jun-1998 [jjk]   fix problems, add more features
20-feb-1998 [jjk]   fix problems, add more features
19-feb-1998 [jjk]   add create/write capabilities
18-feb-1998 [jjk]   from dbfload.py
"""
#------------------------------------------------------------------------------------------------------------------
#from .utils import INVALID_VALUE
#-----------------------------------------------------------------------------------------------------
"""String utilities.

TODO:
  - allow strings in getDateTime routine;
"""
"""History (most recent first):
11-feb-2007 [als]   added INVALID_VALUE
10-feb-2007 [als]   allow date strings padded with spaces instead of zeroes
20-dec-2005 [yc]    handle long objects in getDate/getDateTime
16-dec-2005 [yc]    created from ``strutil`` module.
"""

__version__ = "$Revision: 1.4 $"[11:-2]
__date__ = "$Date: 2007/02/11 08:57:17 $"[7:-2]

import os
import datetime
import time
import glob
from os.path import isfile

def getDirectoryFileList(a_subdir, a_extensionStr):

    globString = ".\\*."  + a_extensionStr
    if (a_subdir != None):
       globString = ".\\" + a_subdir + "\\" + "*."  + a_extensionStr 
    FileList =  glob.glob(globString)
          
    return FileList

def test_getDirectoryFileList():
    fileList = getDirectoryFileList(None,"dbf")
    for eachfile in fileList:
      print (eachfile)
    
def unzfill(str):
    """Return a string without ASCII NULs.

    This function searchers for the first NUL (ASCII 0) occurance
    and truncates string till that position.

    """
    try:
        return str[:str.index(b'\0')]
    except ValueError:
        return str


def getDate(date=None):
    """Return `datetime.date` instance.

    Type of the ``date`` argument could be one of the following:
        None:
            use current date value;
        datetime.date:
            this value will be returned;
        datetime.datetime:
            the result of the date.date() will be returned;
        string:
            assuming "%Y%m%d" or "%y%m%dd" format;
        number:
            assuming it's a timestamp (returned for example
            by the time.time() call;
        sequence:
            assuming (year, month, day, ...) sequence;

    Additionaly, if ``date`` has callable ``ticks`` attribute,
    it will be used and result of the called would be treated
    as a timestamp value.

    """
    if date is None:
        # use current value
        return datetime.date.today()
    if isinstance(date, datetime.date):
        return date
    if isinstance(date, datetime.datetime):
        return date.date()
    if isinstance(date, (int, float)):
        # date is a timestamp
        return datetime.date.fromtimestamp(date)
    if isinstance(date, bytes):
        # decode bytes into a meaningful string.
        date = date.decode()
    if isinstance(date, str):
        date = date.replace(" ", "0")
        if len(date) == 6:
            # yymmdd
            return datetime.date(*time.strptime(date, "%y%m%d")[:3])
        # yyyymmdd
        return datetime.date(*time.strptime(date, "%Y%m%d")[:3])
    if hasattr(date, "__getitem__"):
        # a sequence (assuming date/time tuple)
        return datetime.date(*date[:3])
    return datetime.date.fromtimestamp(date.ticks())


def getDateTime(value=None):
    """Return `datetime.datetime` instance.

    Type of the ``value`` argument could be one of the following:
        None:
            use current date value;
        datetime.date:
            result will be converted to the `datetime.datetime` instance
            using midnight;
        datetime.datetime:
            ``value`` will be returned as is;
        string:
            *** CURRENTLY NOT SUPPORTED ***;
        number:
            assuming it's a timestamp (returned for example
            by the time.time() call;
        sequence:
            assuming (year, month, day, ...) sequence;

    Additionaly, if ``value`` has callable ``ticks`` attribute,
    it will be used and result of the called would be treated
    as a timestamp value.

    """
    if value is None:
        # use current value
        return datetime.datetime.today()
    if isinstance(value, datetime.datetime):
        return value
    if isinstance(value, datetime.date):
        return datetime.datetime.fromordinal(value.toordinal())
    if isinstance(value, (int, float)):
        # value is a timestamp
        return datetime.datetime.fromtimestamp(value)
    if isinstance(value, str):
        raise NotImplementedError("Strings aren't currently implemented")
    if hasattr(value, "__getitem__"):
        # a sequence (assuming date/time tuple)
        return datetime.datetime(*tuple(value)[:6])
    return datetime.datetime.fromtimestamp(value.ticks())


class classproperty(property):
    """Works in the same way as a ``property``, but for the classes."""

    def __get__(self, obj, cls):
        return self.fget(cls)


class _InvalidValue(object):

    """Value returned from DBF records when field validation fails

    The value is not equal to anything except for itself
    and equal to all empty values: None, 0, empty string etc.
    In other words, invalid value is equal to None and not equal
    to None at the same time.

    This value yields zero upon explicit conversion to a number type,
    empty string for string types, and False for boolean.

    """

    def __eq__(self, other):
        return not other

    def __ne__(self, other):
        return not (other is self)

    def __bool__(self):
        return False

    def __int__(self):
        return 0
    __long__ = __int__

    def __float__(self):
        return 0.0

    def __str__(self):
        return ""

    def __unicode__(self):
        return ""

    def __repr__(self):
        return "<INVALID>"

# invalid value is a constant singleton
INVALID_VALUE = _InvalidValue()

# vim: set et sts=4 sw=4 :
__version__ = "$Revision: 1.7 $"[11:-2]
__date__ = "$Date: 2007/02/11 09:23:13 $"[7:-2]
__author__ = "Jeff Kunce <kuncej@mail.conservation.state.mo.us>"
#------------------------------------------------------------------------------------------------------------------------
__all__ = ["Dbf"]
"""DBF fields definitions.

TODO:
  - make memos work
"""
"""History (most recent first):

26-may-2009 [als]   DbfNumericFieldDef.decodeValue: strip zero bytes
05-feb-2009 [als]   DbfDateFieldDef.encodeValue: empty arg produces empty date
16-sep-2008 [als]   DbfNumericFieldDef decoding looks for decimal point
                    in the value to select float or integer return type
13-mar-2008 [als]   check field name length in constructor
11-feb-2007 [als]   handle value conversion errors
10-feb-2007 [als]   DbfFieldDef: added .rawFromRecord()
01-dec-2006 [als]   Timestamp columns use None for empty values
31-oct-2006 [als]   support field types 'F' (float), 'I' (integer)
                    and 'Y' (currency);
                    automate export and registration of field classes
04-jul-2006 [als]   added export declaration
10-mar-2006 [als]   decode empty values for Date and Logical fields;
                    show field name in errors
10-mar-2006 [als]   fix Numeric value decoding: according to spec,
                    value always is string representation of the number;
                    ensure that encoded Numeric value fits into the field
20-dec-2005 [yc]    use field names in upper case
15-dec-2005 [yc]    field definitions moved from `dbf`.
"""

__version__ = "$Revision: 1.14 $"[11:-2]
__date__ = "$Date: 2009/05/26 05:16:51 $"[7:-2]

__all__ = ["lookupFor",] # field classes added at the end of the module

import datetime
import struct
import sys
import locale

#from . import utils

## abstract definitions

class DbfFieldDef(object):
    """Abstract field definition.

    Child classes must override ``type`` class attribute to provide datatype
    infromation of the field definition. For more info about types visit
    `http://www.clicketyclick.dk/databases/xbase/format/data_types.html`

    Also child classes must override ``defaultValue`` field to provide
    default value for the field value.

    If child class has fixed length ``length`` class attribute must be
    overriden and set to the valid value. None value means, that field
    isn't of fixed length.

    Note: ``name`` field must not be changed after instantiation.

    """


    __slots__ = ("name", "decimalCount",
        "start", "end", "ignoreErrors")

    # length of the field, None in case of variable-length field,
    # or a number if this field is a fixed-length field
    length = None

    # field type. for more information about fields types visit
    # `http://www.clicketyclick.dk/databases/xbase/format/data_types.html`
    # must be overriden in child classes
    typeCode = None

    # default value for the field. this field must be
    # overriden in child classes
    defaultValue = None

    def __init__(self, name, length=None, decimalCount=None,
        start=None, stop=None, ignoreErrors=False,
    ):
        """Initialize instance."""
        assert self.typeCode is not None, "Type code must be overriden"
        assert self.defaultValue is not None, "Default value must be overriden"
        ## fix arguments
        if len(name) >10:
            raise ValueError("Field name \"%s\" is too long" % name)
        name = str(name).upper()
        if self.__class__.length is None:
            if length is None:
                raise ValueError("[%s] Length isn't specified" % name)
            length = int(length)
            if length <= 0:
                raise ValueError("[%s] Length must be a positive integer"
                    % name)
        else:
            length = self.length
        if decimalCount is None:
            decimalCount = 0
        ## set fields
        self.name = name
        # FIXME: validate length according to the specification at
        # http://www.clicketyclick.dk/databases/xbase/format/data_types.html
        self.length = length
        self.decimalCount = decimalCount
        self.ignoreErrors = ignoreErrors
        self.start = start
        self.end = stop

    def __cmp__(self, other):
        return cmp(self.name, str(other).upper())

    def __hash__(self):
        return hash(self.name)

    def fromString(cls, string, start, ignoreErrors=False):
        """Decode dbf field definition from the string data.

        Arguments:
            string:
                a string, dbf definition is decoded from. length of
                the string must be 32 bytes.
            start:
                position in the database file.
            ignoreErrors:
                initial error processing mode for the new field (boolean)

        """
        assert len(string) == 32
        _length = string[16]
        #return cls(utils.unzfill(string)[:11].decode(locale.getpreferredencoding()), _length,
        return cls(unzfill(string)[:11].decode(locale.getpreferredencoding()), _length,
            string[17], start, start + _length, ignoreErrors=ignoreErrors)
    fromString = classmethod(fromString)

    def toString(self):
        """Return encoded field definition.

        Return:
            Return value is a string object containing encoded
            definition of this field.

        """
        if sys.version_info < (2, 4):
            # earlier versions did not support padding character
            _name = self.name[:11] + "\0" * (11 - len(self.name))
        else:
            _name = self.name.ljust(11, '\0')
        return (
            _name +
            self.typeCode +
            #data address
            chr(0) * 4 +
            chr(self.length) +
            chr(self.decimalCount) +
            chr(0) * 14
        )


    def __repr__(self):
        return "%-10s %1s %3d %3d" % self.fieldInfo()

    def fieldInfo(self):
        """Return field information.

        Return:
            Return value is a (name, type, length, decimals) tuple.

        """
        return (self.name, self.typeCode, self.length, self.decimalCount)

    def rawFromRecord(self, record):
        """Return a "raw" field value from the record string."""
        return record[self.start:self.end]

    def decodeFromRecord(self, record):
        """Return decoded field value from the record string."""
        try:
            return self.decodeValue(self.rawFromRecord(record))
        except:
            if self.ignoreErrors:
                return utils.INVALID_VALUE
            else:
                raise

    def decodeValue(self, value):
        """Return decoded value from string value.

        This method shouldn't be used publicly. It's called from the
        `decodeFromRecord` method.

        This is an abstract method and it must be overridden in child classes.
        """
        raise NotImplementedError

    def encodeValue(self, value):
        """Return str object containing encoded field value.

        This is an abstract method and it must be overriden in child classes.
        """
        raise NotImplementedError

## real classes

class DbfCharacterFieldDef(DbfFieldDef):
    """Definition of the character field."""

    typeCode = "C"
    defaultValue = b''

    def decodeValue(self, value):
        """Return string object.

        Return value is a ``value`` argument with stripped right spaces.

        """
        return value.rstrip(b' ').decode(locale.getpreferredencoding())

    def encodeValue(self, value):
        """Return raw data string encoded from a ``value``."""
        return str(value)[:self.length].ljust(self.length)


class DbfNumericFieldDef(DbfFieldDef):
    """Definition of the numeric field."""

    typeCode = "N"
    # XXX: now I'm not sure it was a good idea to make a class field
    # `defaultValue` instead of a generic method as it was implemented
    # previously -- it's ok with all types except number, cuz
    # if self.decimalCount is 0, we should return 0 and 0.0 otherwise.
    defaultValue = 0

    def decodeValue(self, value):
        """Return a number decoded from ``value``.

        If decimals is zero, value will be decoded as an integer;
        or as a float otherwise.

        Return:
            Return value is a int (long) or float instance.

        """
        value = value.strip(b' \0')
        if b'.' in value:
            # a float (has decimal separator)
            return float(value)
        elif value:
            # must be an integer
            return int(value)
        else:
            return 0

    def encodeValue(self, value):
        """Return string containing encoded ``value``."""
        _rv = ("%*.*f" % (self.length, self.decimalCount, value))
        if len(_rv) > self.length:
            _ppos = _rv.find(".")
            if 0 <= _ppos <= self.length:
                _rv = _rv[:self.length]
            else:
                raise ValueError("[%s] Numeric overflow: %s (field width: %i)"
                    % (self.name, _rv, self.length))
        return _rv

class DbfFloatFieldDef(DbfNumericFieldDef):
    """Definition of the float field - same as numeric."""

    typeCode = "F"

class DbfIntegerFieldDef(DbfFieldDef):
    """Definition of the integer field."""

    typeCode = "I"
    length = 4
    defaultValue = 0

    def decodeValue(self, value):
        """Return an integer number decoded from ``value``."""
        return struct.unpack("<i", value)[0]

    def encodeValue(self, value):
        """Return string containing encoded ``value``."""
        return struct.pack("<i", int(value))

class DbfCurrencyFieldDef(DbfFieldDef):
    """Definition of the currency field."""

    typeCode = "Y"
    length = 8
    defaultValue = 0.0

    def decodeValue(self, value):
        """Return float number decoded from ``value``."""
        return struct.unpack("<q", value)[0] / 10000.

    def encodeValue(self, value):
        """Return string containing encoded ``value``."""
        return struct.pack("<q", round(value * 10000))

class DbfLogicalFieldDef(DbfFieldDef):
    """Definition of the logical field."""

    typeCode = "L"
    defaultValue = -1
    length = 1

    def decodeValue(self, value):
        """Return True, False or -1 decoded from ``value``."""
        # Note: value always is 1-char string
        if value == "?":
            return -1
        if value in "NnFf ":
            return False
        if value in "YyTt":
            return True
        raise ValueError("[%s] Invalid logical value %r" % (self.name, value))

    def encodeValue(self, value):
        """Return a character from the "TF?" set.

        Return:
            Return value is "T" if ``value`` is True
            "?" if value is -1 or False otherwise.

        """
        if value is True:
            return "T"
        if value == -1:
            return "?"
        return "F"


class DbfMemoFieldDef(DbfFieldDef):
    """Definition of the memo field.

    Note: memos aren't currenly completely supported.

    """

    typeCode = "M"
    defaultValue = " " * 10
    length = 10

    def decodeValue(self, value):
        """Return int .dbt block number decoded from the string object."""
        #return int(value)
        raise NotImplementedError

    def encodeValue(self, value):
        """Return raw data string encoded from a ``value``.

        Note: this is an internal method.

        """
        #return str(value)[:self.length].ljust(self.length)
        raise NotImplementedError


class DbfDateFieldDef(DbfFieldDef):
    """Definition of the date field."""

    typeCode = "D"
    #defaultValue = utils.classproperty(lambda cls: datetime.date.today())
    defaultValue = classproperty(lambda cls: datetime.date.today())
    # "yyyymmdd" gives us 8 characters
    length = 8

    def decodeValue(self, value):
        """Return a ``datetime.date`` instance decoded from ``value``."""
        if value.strip():
            #return utils.getDate(value)
            return getDate(value)
        else:
            return None

    def encodeValue(self, value):
        """Return a string-encoded value.

        ``value`` argument should be a value suitable for the
        `utils.getDate` call.

        Return:
            Return value is a string in format "yyyymmdd".

        """
        if value:
            #return utils.getDate(value).strftime("%Y%m%d")
            return getDate(value).strftime("%Y%m%d")
        else:
            return " " * self.length


class DbfDateTimeFieldDef(DbfFieldDef):
    """Definition of the timestamp field."""

    # a difference between JDN (Julian Day Number)
    # and GDN (Gregorian Day Number). note, that GDN < JDN
    JDN_GDN_DIFF = 1721425
    typeCode = "T"
    #defaultValue = utils.classproperty(lambda cls: datetime.datetime.now())
    defaultValue = classproperty(lambda cls: datetime.datetime.now())
    # two 32-bits integers representing JDN and amount of
    # milliseconds respectively gives us 8 bytes.
    # note, that values must be encoded in LE byteorder.
    length = 8

    def decodeValue(self, value):
        """Return a `datetime.datetime` instance."""
        assert len(value) == self.length
        # LE byteorder
        _jdn, _msecs = struct.unpack("<2I", value)
        if _jdn >= 1:
            _rv = datetime.datetime.fromordinal(_jdn - self.JDN_GDN_DIFF)
            _rv += datetime.timedelta(0, _msecs / 1000.0)
        else:
            # empty date
            _rv = None
        return _rv

    def encodeValue(self, value):
        """Return a string-encoded ``value``."""
        if value:
            value = utils.getDateTime(value)
            # LE byteorder
            _rv = struct.pack("<2I", value.toordinal() + self.JDN_GDN_DIFF,
                (value.hour * 3600 + value.minute * 60 + value.second) * 1000)
        else:
            _rv = "\0" * self.length
        assert len(_rv) == self.length
        return _rv


_fieldsRegistry = {}

def registerField(fieldCls):
    """Register field definition class.

    ``fieldCls`` should be subclass of the `DbfFieldDef`.

    Use `lookupFor` to retrieve field definition class
    by the type code.

    """
    assert fieldCls.typeCode is not None, "Type code isn't defined"
    # XXX: use fieldCls.typeCode.upper()? in case of any decign
    # don't forget to look to the same comment in ``lookupFor`` method
    
    _fieldsRegistry[fieldCls.typeCode] = fieldCls


def lookupFor(typeCode):
    """Return field definition class for the given type code.

    ``typeCode`` must be a single character. That type should be
    previously registered.

    Use `registerField` to register new field class.

    Return:
        Return value is a subclass of the `DbfFieldDef`.

    """
    # XXX: use typeCode.upper()? in case of any decign don't
    # forget to look to the same comment in ``registerField``
    
    if isinstance(typeCode,int) :
        return _fieldsRegistry[chr(typeCode)] #original code
    else : # assuming str
        return _fieldsRegistry[typeCode]   

## register generic types

for (_name, _val) in list(globals().items()):
    if isinstance(_val, type) and issubclass(_val, DbfFieldDef) \
    and (_name != "DbfFieldDef"):
        __all__.append(_name)
        registerField(_val)
del _name, _val

# vim: et sts=4 sw=4 :

#=========================================================================================================================
#    DBF header
#=========================================================================================================================
"""DBF header definition.

TODO:
  - handle encoding of the character fields
    (encoding information stored in the DBF header)

"""
"""History (most recent first):
09-Jun-2019 [psc]   write: Fix Field length greater than 128 error (it went negative)
16-sep-2010 [als]   fromStream: fix century of the last update field
11-feb-2007 [als]   added .ignoreErrors
10-feb-2007 [als]   added __getitem__: return field definitions
                    by field name or field number (zero-based)
04-jul-2006 [als]   added export declaration
15-dec-2005 [yc]    created
"""

__version__ = "$Revision: 1.6 $"[11:-2]
__date__ = "$Date: 2010/09/16 05:06:39 $"[7:-2]

__all__ = ["DbfHeader"]

import io
import datetime
import struct
import time
import sys

#from . import fields
#from .utils import getDate


class DbfHeader(object):
    """Dbf header definition.

    For more information about dbf header format visit
    `http://www.clicketyclick.dk/databases/xbase/format/dbf.html#DBF_STRUCT`

    Examples:
        Create an empty dbf header and add some field definitions:
            dbfh = DbfHeader()
            dbfh.addField(("name", "C", 10))
            dbfh.addField(("date", "D"))
            dbfh.addField(DbfNumericFieldDef("price", 5, 2))
        Create a dbf header with field definitions:
            dbfh = DbfHeader([
                ("name", "C", 10),
                ("date", "D"),
                DbfNumericFieldDef("price", 5, 2),
            ])

    """

    __slots__ = ("signature", "fields", "lastUpdate", "recordLength",
        "recordCount", "headerLength", "changed", "_ignore_errors")

    ## instance construction and initialization methods

    def __init__(self, fields=None, headerLength=0, recordLength=0,
        recordCount=0, signature=0x03, lastUpdate=None, ignoreErrors=False,
    ):
        """Initialize instance.

        Arguments:
            fields:
                a list of field definitions;
            recordLength:
                size of the records;
            headerLength:
                size of the header;
            recordCount:
                number of records stored in DBF;
            signature:
                version number (aka signature). using 0x03 as a default meaning
                "File without DBT". for more information about this field visit
                ``http://www.clicketyclick.dk/databases/xbase/format/dbf.html#DBF_NOTE_1_TARGET``
            lastUpdate:
                date of the DBF's update. this could be a string ('yymmdd' or
                'yyyymmdd'), timestamp (int or float), datetime/date value,
                a sequence (assuming (yyyy, mm, dd, ...)) or an object having
                callable ``ticks`` field.
            ignoreErrors:
                error processing mode for DBF fields (boolean)

        """
        self.signature = signature
        if fields is None:
            self.fields = []
        else:
            self.fields = list(fields)
        self.lastUpdate = getDate(lastUpdate)
        self.recordLength = recordLength
        self.headerLength = headerLength
        self.recordCount = recordCount
        self.ignoreErrors = ignoreErrors
        # XXX: I'm not sure this is safe to
        # initialize `self.changed` in this way
        self.changed = bool(self.fields)

    # @classmethod
    def fromString(cls, string):
        """Return header instance from the string object."""
        return cls.fromStream(io.StringIO(str(string)))
    fromString = classmethod(fromString)

    # @classmethod
    def fromStream(cls, stream):
        """Return header object from the stream."""
        stream.seek(0)
        first_32 = stream.read(32)
        if type(first_32) != bytes:
            _data = bytes(first_32, sys.getfilesystemencoding())
        _data = first_32
        (_cnt, _hdrLen, _recLen) = struct.unpack("<I2H", _data[4:12])
        #reserved = _data[12:32]
        _year = _data[1]
        if _year < 80:
            # dBase II started at 1980.  It is quite unlikely
            # that actual last update date is before that year.
            _year += 2000
        else:
            _year += 1900
        ## create header object
        _obj = cls(None, _hdrLen, _recLen, _cnt, _data[0],
            (_year, _data[2], _data[3]))
        ## append field definitions
        # position 0 is for the deletion flag
        _pos = 1
        _data = stream.read(1)
        while _data != b'\r':
            _data += stream.read(31)
            #_fld = fields.lookupFor(_data[11]).fromString(_data, _pos)
            _fld = lookupFor(_data[11]).fromString(_data, _pos)
            _obj._addField(_fld)
            _pos = _fld.end
            _data = stream.read(1)
        return _obj
    fromStream = classmethod(fromStream)

    ## properties

    year = property(lambda self: self.lastUpdate.year)
    month = property(lambda self: self.lastUpdate.month)
    day = property(lambda self: self.lastUpdate.day)

    def ignoreErrors(self, value):
        """Update `ignoreErrors` flag on self and all fields"""
        self._ignore_errors = value = bool(value)
        for _field in self.fields:
            _field.ignoreErrors = value
    ignoreErrors = property(
        lambda self: self._ignore_errors,
        ignoreErrors,
        doc="""Error processing mode for DBF field value conversion

        if set, failing field value conversion will return
        ``INVALID_VALUE`` instead of raising conversion error.

        """)

    ## object representation

    def __repr__(self):
        _rv = """\
Version (signature): 0x%02x
        Last update: %s
      Header length: %d
      Record length: %d
       Record count: %d
 FieldName Type Len Dec
""" % (self.signature, self.lastUpdate, self.headerLength,
    self.recordLength, self.recordCount)
        _rv += "\n".join(
            ["%10s %4s %3s %3s" % _fld.fieldInfo() for _fld in self.fields]
        )
        return _rv

    ## internal methods

    def _addField(self, *defs):
        """Internal variant of the `addField` method.

        This method doesn't set `self.changed` field to True.

        Return value is a length of the appended records.
        Note: this method doesn't modify ``recordLength`` and
        ``headerLength`` fields. Use `addField` instead of this
        method if you don't exactly know what you're doing.

        """
        # insure we have dbf.DbfFieldDef instances first (instantiation
        # from the tuple could raise an error, in such a case I don't
        # wanna add any of the definitions -- all will be ignored)
        _defs = []
        _recordLength = 0
        for _def in defs:
            #if isinstance(_def, fields.DbfFieldDef):
            if isinstance(_def, DbfFieldDef):
                _obj = _def
            else:
                (_name, _type, _len, _dec) = (tuple(_def) + (None,) * 4)[:4]
                #_cls = fields.lookupFor(_type)
                _cls = lookupFor(_type)
                _obj = _cls(_name, _len, _dec,
                    ignoreErrors=self._ignore_errors)
            _recordLength += _obj.length
            _defs.append(_obj)
        # and now extend field definitions and
        # update record length
        self.fields += _defs
        return _recordLength

    ## interface methods

    def addField(self, *defs):
        """Add field definition to the header.

        Examples:
            dbfh.addField(
                ("name", "C", 20),
                dbf.DbfCharacterFieldDef("surname", 20),
                dbf.DbfDateFieldDef("birthdate"),
                ("member", "L"),
            )
            dbfh.addField(("price", "N", 5, 2))
            dbfh.addField(dbf.DbfNumericFieldDef("origprice", 5, 2))

        """
        _oldLen = self.recordLength
        self.recordLength += self._addField(*defs)
        if not _oldLen:
            self.recordLength += 1
            # XXX: may be just use:
            # self.recordeLength += self._addField(*defs) + bool(not _oldLen)
        # recalculate headerLength
        self.headerLength = 32 + (32 * len(self.fields)) + 1
        self.changed = True

    def write(self, stream):
        """Encode and write header to the stream."""
        stream.seek(0)
        stream.write(self.toString())
                
        fields = [_fld.toString() for _fld in self.fields]

        #fix bug ChiuPikS
        for vfield in fields :  
           for eachChar in vfield :
             stream.write(bytes([ord(eachChar)]))
        # original code: the following original code only works if the field length is less than 128.     
        #stream.write(''.join(fields).encode(sys.getfilesystemencoding()))
        stream.write(b'\x0D')   # cr at end of all header data
        self.changed = False

    def toString(self):
        """Returned 32 chars length string with encoded header."""
        return struct.pack("<4BI2H",
            self.signature,
            self.year - 1900,
            self.month,
            self.day,
            self.recordCount,
            self.headerLength,
            self.recordLength) + (b'\x00' * 20)
        #TODO: figure out if bytes(utf-8) is correct here.

    def setCurrentDate(self):
        """Update ``self.lastUpdate`` field with current date value."""
        self.lastUpdate = datetime.date.today()

    def __getitem__(self, item):
        """Return a field definition by numeric index or name string"""
        if isinstance(item, str):
            _name = item.upper()
            for _field in self.fields:
                if _field.name == _name:
                    return _field
            else:
                raise KeyError(item)
        else:
            # item must be field index
            return self.fields[item]

          
#------------end of field -----------------------------------------------------------------------------------------

# vim: et sts=4 sw=4 :
#-----------------------------------------------------------------------------------------------------
#from . import record
#-----------------------------------------------------------------------------------------------------
"""DBF record definition.

"""
"""History (most recent first):
11-feb-2007 [als]   __repr__: added special case for invalid field values
10-feb-2007 [als]   added .rawFromStream()
30-oct-2006 [als]   fix record length in .fromStream()
04-jul-2006 [als]   added export declaration
20-dec-2005 [yc]    DbfRecord.write() -> DbfRecord._write();
                    added delete() method.
16-dec-2005 [yc]    record definition moved from `dbf`.
"""

__version__ = "$Revision: 1.7 $"[11:-2]
__date__ = "$Date: 2007/02/11 09:05:49 $"[7:-2]

__all__ = ["DbfRecord"]

#import sys

#from . import utils

class DbfRecord(object):
    """DBF record.

    Instances of this class shouldn't be created manualy,
    use `dbf.Dbf.newRecord` instead.

    Class implements mapping/sequence interface, so
    fields could be accessed via their names or indexes
    (names is a preffered way to access fields).

    Hint:
        Use `store` method to save modified record.

    Examples:
        Add new record to the database:
            db = Dbf(filename)
            rec = db.newRecord()
            rec["FIELD1"] = value1
            rec["FIELD2"] = value2
            rec.store()
        Or the same, but modify existed
        (second in this case) record:
            db = Dbf(filename)
            rec = db[2]
            rec["FIELD1"] = value1
            rec["FIELD2"] = value2
            rec.store()

    """

    __slots__ = "dbf", "index", "deleted", "fieldData"

    ## creation and initialization

    def __init__(self, dbf, index=None, deleted=False, data=None):
        """Instance initialiation.

        Arguments:
            dbf:
                A `Dbf.Dbf` instance this record belonogs to.
            index:
                An integer record index or None. If this value is
                None, record will be appended to the DBF.
            deleted:
                Boolean flag indicating whether this record
                is a deleted record.
            data:
                A sequence or None. This is a data of the fields.
                If this argument is None, default values will be used.

        """
        self.dbf = dbf
        # XXX: I'm not sure ``index`` is necessary
        self.index = index
        self.deleted = deleted
        if data is None:
            self.fieldData = [_fd.defaultValue for _fd in dbf.header.fields]
        else:
            self.fieldData = list(data)

    # XXX: validate self.index before calculating position?
    position = property(lambda self: self.dbf.header.headerLength + \
        self.index * self.dbf.header.recordLength)

    def rawFromStream(cls, dbf, index):
        """Return raw record contents read from the stream.

        Arguments:
            dbf:
                A `Dbf.Dbf` instance containing the record.
            index:
                Index of the record in the records' container.
                This argument can't be None in this call.

        Return value is a string containing record data in DBF format.

        """
        # XXX: may be write smth assuming, that current stream
        # position is the required one? it could save some
        # time required to calculate where to seek in the file
        dbf.stream.seek(dbf.header.headerLength +
            index * dbf.header.recordLength)
        return dbf.stream.read(dbf.header.recordLength)
    rawFromStream = classmethod(rawFromStream)

    def fromStream(cls, dbf, index):
        """Return a record read from the stream.

        Arguments:
            dbf:
                A `Dbf.Dbf` instance new record should belong to.
            index:
                Index of the record in the records' container.
                This argument can't be None in this call.

        Return value is an instance of the current class.

        """
        return cls.fromString(dbf, cls.rawFromStream(dbf, index), index)
    fromStream = classmethod(fromStream)

    def fromString(cls, dbf, string, index=None):
        """Return record read from the string object.

        Arguments:
            dbf:
                A `Dbf.Dbf` instance new record should belong to.
            string:
                A string new record should be created from.
            index:
                Index of the record in the container. If this
                argument is None, record will be appended.

        Return value is an instance of the current class.

        """
        return cls(dbf, index, string[0]=="*",
            [_fd.decodeFromRecord(string) for _fd in dbf.header.fields])
    fromString = classmethod(fromString)

    ## object representation

    def __repr__(self):
        _template = "%%%ds: %%s (%%s)" % max([len(_fld)
            for _fld in self.dbf.fieldNames])
        _rv = []
        for _fld in self.dbf.fieldNames:
            _val = self[_fld]
            #if _val is utils.INVALID_VALUE:
            if _val is INVALID_VALUE:
                _rv.append(_template %
                    (_fld, "None", "value cannot be decoded"))
            else:
                _rv.append(_template % (_fld, _val, type(_val)))
        return "\n".join(_rv)

    ## protected methods

    def _write(self):
        """Write data to the dbf stream.

        Note:
            This isn't a public method, it's better to
            use 'store' instead publically.
            Be design ``_write`` method should be called
            only from the `Dbf` instance.


        """
        self._validateIndex(False)
        self.dbf.stream.seek(self.position)
        self.dbf.stream.write(bytes(self.toString(),
            sys.getfilesystemencoding()))
        # FIXME: may be move this write somewhere else?
        # why we should check this condition for each record?
        if self.index == len(self.dbf):
            # this is the last record,
            # we should write SUB (ASCII 26)
            self.dbf.stream.write(b"\x1A")

    ## utility methods

    def _validateIndex(self, allowUndefined=True, checkRange=False):
        """Valid ``self.index`` value.

        If ``allowUndefined`` argument is True functions does nothing
        in case of ``self.index`` pointing to None object.

        """
        if self.index is None:
            if not allowUndefined:
                raise ValueError("Index is undefined")
        elif self.index < 0:
            raise ValueError("Index can't be negative (%s)" % self.index)
        elif checkRange and self.index <= self.dbf.header.recordCount:
            raise ValueError("There are only %d records in the DBF" %
                self.dbf.header.recordCount)

    ## interface methods

    def store(self):
        """Store current record in the DBF.

        If ``self.index`` is None, this record will be appended to the
        records of the DBF this records belongs to; or replaced otherwise.

        """
        self._validateIndex()
        if self.index is None:
            self.index = len(self.dbf)
            self.dbf.append(self)
        else:
            self.dbf[self.index] = self

    def delete(self):
        """Mark method as deleted."""
        self.deleted = True

    def toString(self):
        """Return string packed record values."""

        return "".join([" *"[self.deleted]] + [
           _def.encodeValue(_dat)
            for (_def, _dat) in zip(self.dbf.header.fields, self.fieldData)
        ])

    def asList(self):
        """Return a flat list of fields.

        Note:
            Change of the list's values won't change
            real values stored in this object.

        """
        return self.fieldData[:]

    def asDict(self):
        """Return a dictionary of fields.

        Note:
            Change of the dicts's values won't change
            real values stored in this object.

        """
        return dict([_i for _i in zip(self.dbf.fieldNames, self.fieldData)])

    def __getitem__(self, key):
        """Return value by field name or field index."""
        if isinstance(key, int):
            # integer index of the field
            return self.fieldData[key]
        # assuming string field name
        return self.fieldData[self.dbf.indexOfFieldName(key)]

    def __setitem__(self, key, value):
        """Set field value by integer index of the field or string name."""
        if isinstance(key, int):
            # integer index of the field
            return self.fieldData[key]
        # assuming string field name
        self.fieldData[self.dbf.indexOfFieldName(key)] = value

# vim: et sts=4 sw=4 :

#------------end of record-----------------------------------------------------------------------------------------

#------ start of DBF --------------------------------------------------------------------------------------------
class Dbf(object):
    """DBF accessor.

    FIXME:
        docs and examples needed (dont' forget to tell
        about problems adding new fields on the fly)

    Implementation notes:
        ``_new`` field is used to indicate whether this is
        a new data table. `addField` could be used only for
        the new tables! If at least one record was appended
        to the table it's structure couldn't be changed.

    """

    __slots__ = ("name", "header", "stream",
        "_changed", "_new", "_ignore_errors")

    #HeaderClass = header.DbfHeader
    #RecordClass = record.DbfRecord
    HeaderClass = DbfHeader
    RecordClass = DbfRecord
    INVALID_VALUE = INVALID_VALUE

    ## initialization and creation helpers

    def __init__(self, f, readOnly=False, new=False, ignoreErrors=False):
        """Initialize instance.

        Arguments:
            f:
                Filename or file-like object.
            new:
                True if new data table must be created. Assume
                data table exists if this argument is False.
            readOnly:
                if ``f`` argument is a string file will
                be opend in read-only mode; in other cases
                this argument is ignored. This argument is ignored
                even if ``new`` argument is True.
            headerObj:
                `header.DbfHeader` instance or None. If this argument
                is None, new empty header will be used with the
                all fields set by default.
            ignoreErrors:
                if set, failing field value conversion will return
                ``INVALID_VALUE`` instead of raising conversion error.

        """
        if isinstance(f, str):
            # a filename
            self.name = f
            if new:
                # new table (table file must be
                # created or opened and truncated)
                self.stream = open(f, "w+b")
            else:
                # tabe file must exist
                self.stream = open(f, ("r+b", "rb")[bool(readOnly)])
        else:
            # a stream
            self.name = getattr(f, "name", "")
            self.stream = f
        if new:
            # if this is a new table, header will be empty
            self.header = self.HeaderClass()
        else:
            # or instantiated using stream
            self.header = self.HeaderClass.fromStream(self.stream)
        self.ignoreErrors = ignoreErrors
        self._new = bool(new)
        self._changed = False

    ## properties

    closed = property(lambda self: self.stream.closed)
    recordCount = property(lambda self: self.header.recordCount)
    fieldNames = property(
        lambda self: [_fld.name for _fld in self.header.fields])
    fieldDefs = property(lambda self: self.header.fields)
    changed = property(lambda self: self._changed or self.header.changed)

    def ignoreErrors(self, value):
        """Update `ignoreErrors` flag on the header object and self"""
        self.header.ignoreErrors = self._ignore_errors = bool(value)
    ignoreErrors = property(
        lambda self: self._ignore_errors,
        ignoreErrors,
        doc="""Error processing mode for DBF field value conversion

        if set, failing field value conversion will return
        ``INVALID_VALUE`` instead of raising conversion error.

        """)

    ## protected methods
    def _fixIndex(self, index):
        """Return fixed index.

        This method fails if index isn't a numeric object
        (long or int). Or index isn't in a valid range
        (less or equal to the number of records in the db).

        If ``index`` is a negative number, it will be
        treated as a negative indexes for list objects.

        Return:
            Return value is numeric object maning valid index.

        """
        if not isinstance(index, int):
            raise TypeError("Index must be a numeric object")
        if index < 0:
            # index from the right side
            # fix it to the left-side index
            index += len(self) + 1
        if index >= len(self):
            raise IndexError("Record index out of range")
        return index

    ## iterface methods

    def close(self):
        self.flush()
        self.stream.close()

    def flush(self):
        """Flush data to the associated stream."""
        if self.changed:
            self.header.setCurrentDate()
            self.header.write(self.stream)
            self.stream.flush()
            self._changed = False

    def indexOfFieldName(self, name):
        """Index of field named ``name``."""
        # FIXME: move this to header class
        names = [f.name for f in self.header.fields]
        return names.index(name.upper())

    def newRecord(self):
        """Return new record, which belong to this table."""
        return self.RecordClass(self)

    def append(self, record):
        """Append ``record`` to the database."""
        record.index = self.header.recordCount
        record._write()
        self.header.recordCount += 1
        self._changed = True
        self._new = False

    def addField(self, *defs):
        """Add field definitions.

        For more information see `header.DbfHeader.addField`.

        """
        if self._new:
            self.header.addField(*defs)
        else:
            raise TypeError("At least one record was added, "
                "structure can't be changed")
   
    ## 'magic' methods (representation and sequence interface)

    def __repr__(self):
        return "Dbf stream '%s'\n" % self.stream + repr(self.header)

    def __len__(self):
        """Return number of records."""
        return self.recordCount

    def __getitem__(self, index):
        """Return `DbfRecord` instance."""
        return self.RecordClass.fromStream(self, self._fixIndex(index))

    def __setitem__(self, index, record):
        """Write `DbfRecord` instance to the stream."""
        record.index = self._fixIndex(index)
        record._write()
        self._changed = True
        self._new = False

    #def __del__(self):
    #    """Flush stream upon deletion of the object."""
    #    self.flush()


#==========================================================================================
#==========      ChiuPikS additions =======================================================
#==========================================================================================
    def getFileName(self):
        return self.name

    def GetFieldNameList(self) :
      """
      Same as follows:
      fieldNameList = []
      for f in self.header.fields:
         fieldNameList.append(f.name)
         
      return fieldNameList
      """
      names = [f.name for f in self.header.fields]
      return names
    #=================================================================================
    def GetNumberOfRecords(self):
        return self.recordCount
    #=================================================================================
    def AppendRecord(self, a_record):
        fieldNameList = self.fieldNames
        append_rec = self.newRecord()
        
        count = 0
        for fieldName in fieldNameList :
           append_rec[fieldName] = a_record[count]
           count += 1

        append_rec.store()
    #=================================================================================
    def CopyRecord(self, a_record):
        fieldNameList = self.fieldNames
        append_rec = self.newRecord()

        count = 0
        for fieldName in fieldNameList :
           append_rec[fieldName] = a_record[count]
           count += 1

        return append_rec
    #=================================================================================
    def CreateFieldsFromFile(self, input_filename):

        in_dbf = Dbf(input_filename, True)
        #in_fieldNameList = in_dbf.fieldNames

        for f in in_dbf.header.fields:
          self.addField(f)
       
        in_dbf.close()
        #self.close()
    #=================================================================================
    def AppendRecordsFromFile(self, input_filename):

        in_dbf = Dbf(input_filename, True)
        in_fieldNameList = in_dbf.fieldNames

        for in_rec in in_dbf:
           append_rec = self.newRecord()
           
           for fieldName in in_fieldNameList :
             append_rec[fieldName] = in_rec[fieldName]
           append_rec.store()
      
        in_dbf.close()
        #self.close()
       
    #=================================================================================
    def printDbf(self):
      for _rec in self:
        print()
        print(repr(_rec))
    #=================================================================================
    def printRecord(self,a_rec):
        print()
        print(repr(a_rec))
    #=================================================================================
    def GetDefinitions(self):
        return self.fieldDefs
    #=================================================================================
    def FieldNameExists(self, a_fieldName):
        bFieldNameExist = False
        
        for eachFieldName in self.fieldNames :
            if eachFieldName == a_fieldName :
                bFieldNameExist = True
                break
            
        if not bFieldNameExist :
            print("No such field name:",a_fieldName)
            
        return bFieldNameExist
    #=================================================================================
    def FindRecords(self,a_fieldName,a_searchItemList):
        bFound = False
        searchStr = ""
        for searchItem in a_searchItemList :
            searchStr += searchItem
            searchStr += " "
            
        printStr = "=== List all the content in Column " + a_fieldName + " which contains " + searchStr  + " ==="
        foundList = []
        foundList.append(printStr)
        
        if self.FieldNameExists(a_fieldName):
            
          for eachRecord in self:
             for searchStr in a_searchItemList:
               if (eachRecord[a_fieldName].find(searchStr) >= 0) :
                   bFound = True
                   #print (eachRecord[a_fieldName])
                   foundList.append(eachRecord[a_fieldName])
                 
        return foundList
    #=================================================================================
    def GetRecord(self,a_rowNum):
        appRecord = None
        if (self.recordCount >= a_rowNum):
          appRecord = CopyRecord(self[a_rowNum])
        else :
          print ("GetRecord(): given record count ",a_rowNum, " is bigger than the database size ",self.recordCount)

        return appRecord
   #===================================================================================================
    def SortColumn(self, a_fieldName):
       rowCount = 0
       zeroStr = ""
       columnList = []
       for eachRecord in self :
           if rowCount < 10 :
               zeroStr = "0000"
           elif rowCount < 100 :
               zeroStr = "000"
           elif rowCount < 1000 :
               zeroStr = "00"
           elif rowCount < 10000 :
               zeroStr = "0"
               
           colValue = eachRecord[a_fieldName] + "," + zeroStr + str(rowCount)
           columnList.append(colValue)
           rowCount += 1          
           
       columnList.sort()
       
       output_filename = self.name.replace(".","Sort.")
       out_dbf = Dbf(output_filename, new=True)
    
       for f in self.header.fields:
           out_dbf.addField(f)
       
       for item in columnList :
           itemList = item.split(",")  # itemList contains [columnValue, rowIndex]
           rowIndex = int(itemList[1])

           out_dbf.AppendRecord(self[rowIndex])
       out_dbf.close()
       
       return output_filename
   #===================================================================================================
    def SortColumnAndCreateSortFiles(self, a_fieldName):
       
       #------------------------------------------------------------------------------------- 
       def GenerateNextSortFileName(a_fileStr, a_fieldStr) :
           out_fileStr = ""
           SortStr = "Sort_" + a_fieldStr.replace(" ","_") + "_"
           
           if (a_fileStr.find("Sort_") >= 0):
              out_fileStr = a_fileStr.replace("Sort_",SortStr)
           else :
              out_fileStr = SortStr + a_fileStr
              
           return out_fileStr
       #------------------------------------------------------------------------------------- 
            
       rowCount = 0
       zeroStr = ""
       columnList = []
       splitFileList = []
       output_filename = ""
       fieldStr = None
       out_dbf = None
       
       for eachRecord in self :
           if rowCount < 10 :
               zeroStr = "0000"
           elif rowCount < 100 :
               zeroStr = "000"
           elif rowCount < 1000 :
               zeroStr = "00"
           elif rowCount < 10000 :
               zeroStr = "0"
               
           colValue = eachRecord[a_fieldName] + "," + zeroStr + str(rowCount)
           columnList.append(colValue)
           rowCount += 1
           
       columnList.sort()
   
       for item in columnList :
           itemList = item.split(",")  # itemList contains [FieldValue, rowIndex]
           rowIndex = int(itemList[1])
           bCreateNextFile = False
           if fieldStr == None :
               fieldStr = itemList[0]
               bCreateNextFile = True
           elif fieldStr != itemList[0] :
               out_dbf.close()
               fieldStr = itemList[0]
               bCreateNextFile = True

           if  bCreateNextFile :       
               output_filename = GenerateNextSortFileName(self.name, fieldStr)

               splitFileList.append(output_filename)
               out_dbf = Dbf(output_filename, new=True)
               for f in self.header.fields:
                  out_dbf.addField(f) 

           out_dbf.AppendRecord(self[rowIndex])
       out_dbf.close()
       return splitFileList
   #===================================================================================================

    def FindAndReplaceRecords(self, a_searchFieldName, a_searchItemList, a_replaceStr, a_replaceOtherFieldList = None):
      """
      Description:
         If any of the search strings in a_searchItemList exists in the a_fieldName
         then replace the found string with the a_replaceStr.
         If the a_replaceOtherFieldList exists,
         then also replace the field  with the field data in the a_replaceOtherFieldList
      Parameters :
        a_searchFieldName : The fieldName string to the searched
                           such as "Fruit"  
        a_searchItemList : search Item List
                         such as ["banana","apple","lemon"]
        a_replaceStr : replace string
                         such as "grape"
        a_replaceOtherFieldList :
                         such as [("color","blue"),("size","big")]

        eg replace all the banana, apple, lemon at the Fruit column with grape
           and set the 'color' column to blue and 'size' column to big
      """
      if a_replaceOtherFieldList != None:
          for fieldStr, replaceString in a_replaceOtherFieldList :
              if not self.FieldNameExists(fieldStr):
                 print ("The field ",fieldStr, " does not exist.")
                 return
      dirName = "ReplacementDir"        
      if self.FieldNameExists(a_searchFieldName):
          
          if not os.path.exists(dirName):
              os.mkdir(dirName)
              
          in_fieldNameList = self.fieldNames
          output_filename = dirName + "\\" + self.name
          out_dbf = Dbf(output_filename, new=True)
    
          for f in self.header.fields:
             out_dbf.addField(f)

          for in_rec in self:
             out_rec = out_dbf.newRecord()
             bFoundSearchStr = False
             
             for fieldName in self.fieldNames :
               out_rec[fieldName] = in_rec[fieldName]
           
               if (fieldName.find(a_searchFieldName) >= 0):
                  if (a_searchItemList != None) :
                      foundSearchStr = None
                      bFoundSearchStr = False
                  
                      for searchItem in a_searchItemList :
                          if (out_rec[a_searchFieldName].find(searchItem) >= 0):
                              foundSearchStr = searchItem
                              bFoundSearchStr = True
                              break
                        
                      if (foundSearchStr != None) :
                          if (a_replaceStr != None) :
                              out_rec[fieldName] = in_rec[fieldName].replace(foundSearchStr,a_replaceStr)
                          
             if bFoundSearchStr and (a_replaceOtherFieldList != None) :    
                for otherFieldStr, otherReplaceStr in a_replaceOtherFieldList :
                    out_rec[otherFieldStr] = otherReplaceStr
              
             out_rec.store()
     
          out_dbf.close()
#=========================================================
def demoRead(filename):
    print("=============demoRead========================")
    _dbf = Dbf(filename, True)
    #------ get field names ---------------
    print (" 3rd record surname:",_dbf[2]["SURNAME"])
    FieldNameList = _dbf.GetFieldNameList()
    
    print ("All the field names :",FieldNameList)
    
    fieldNameList2 = _dbf.fieldNames
    for eachName in fieldNameList2 :
      print ("field name:",eachName)
    #------ get records -------------------
    for _rec in _dbf:
        print()
        eachRec = repr(_rec)
        vname = _rec["NAME"]
        vsurname = _rec["SURNAME"]
        print ( "name is ", vname)
        print ( "surname is ", vsurname)
        print(repr(_rec))
    _dbf.close()
#=========================================================
def demoCreate(filename):
    print("=============demoCreate========================")
    _dbf = Dbf(filename, new=True)
    _dbf.addField(
        ("NAME", "C", 15),
        ("SURNAME", "C", 25),
        ("INITIALS", "C", 10),
        ("BIRTHDATE", "D",8), # original code was ("BIRTHDATE", "D",8)
    )
    
    _dbf.addField(("AMOUNT", "N",10,2))   #added by tomc
    
    for (_n, _s, _i, _b,_m) in (
        ("John", "Miller", "YC", (1981, 1, 2),10.1),
        ("Andy", "Larkin", "AL", (1982, 3, 4),10.2),
        ("Bill", "Clinth", "", (1983, 5, 6),10.3),
        ("Bobb", "McNail", "", (1984, 7, 8),10.4),
    ):
        _rec = _dbf.newRecord()
        _rec["NAME"] = _n
        _rec["SURNAME"] = _s
        _rec["INITIALS"] = _i
        _rec["BIRTHDATE"] = _b
        _rec["AMOUNT"] = _m
        _rec.store()
    print(repr(_dbf))
    _dbf.close()
#===================================================================================================
def test_SortColumn() :
      print ("==== start test_SortColumn ========")
      read_dbf = Dbf("SortTest.dbf",True)
      read_dbf.SortColumn("SURNAME")
      read_dbf.close()
#===================================================================================================
def SortDbfFile(a_dbfFile,a_fieldName) :
      read_dbf = Dbf(a_dbfFile,True)
      sortfileName = read_dbf.SortColumn(a_fieldName)
      read_dbf.close()
      return sortfileName
#===================================================================================================
def test_SortColumnAndCreateSortFiles() :
      print ("==== start test_SortColumnAndCreateSortFiles ========")
      read_dbf = Dbf("SortTest.dbf",True)
      read_dbf.SortColumnAndCreateSortFiles("SURNAME")
      read_dbf.close()
#===================================================================================================
def SortAndSplitFile(a_dbf_File,a_fieldName) :
      read_dbf = Dbf(a_dbf_File,True)
      splitFileList = read_dbf.SortColumnAndCreateSortFiles(a_fieldName)
      read_dbf.close()
      return splitFileList
#===================================================================================================
def SortDbfFileByMultiFields(a_dbfFile, a_field1, a_field2 = None, a_field3 = None, a_bSortsplitList1 = True) :
    #-----------------------------------------------------------
    def SortAndSplit(a_fileList, a_fieldStr) :
        splitFileListAll = []
        for vfile in a_fileList :
            splitFileList = SortAndSplitFile(vfile,a_fieldStr)
            splitFileListAll += splitFileList

        for rfile in a_fileList:
          os.remove(rfile)
          
        return splitFileListAll
    #-----------------------------------------------------------
    if a_field2 == None and a_field3 == None :
       SortDbfFile(a_dbfFile,a_field1)
    else :   
        splitFileList2 = []
        splitFileList1 = SortAndSplitFile(a_dbfFile,a_field1)
    
        if a_bSortsplitList1:
            splitFileList1.sort()
    
        if a_field2 != None and a_field3 == None :
        
            sortedFileList1 = []
            for vfile in splitFileList1 :
               sortFileStr = SortDbfFile(vfile,a_field2)
               print (vfile,"     ",sortFileStr)
               sortedFileList1.append(sortFileStr)

            MergeFiles(a_dbfFile.replace(".","_Sorted2."), sortedFileList1)

            for sortfile in splitFileList1:
              if isfile(sortfile):
                print ("remove ",sortfile)
                os.remove(sortfile)

            for sortfile in sortedFileList1:
              if isfile(sortfile):
                print ("remove ",sortfile)
                os.remove(sortfile)
        
        elif a_field2 != None and a_field3 != None :
            splitFileList2 = SortAndSplit(splitFileList1,a_field2)
        
            sortedFileList2 = []  
            for vfile in splitFileList2 :
               sortFileStr = SortDbfFile(vfile,a_field3)
               sortedFileList2.append(sortFileStr)
           
            MergeFiles(a_dbfFile.replace(".","_Sorted3."), sortedFileList2)
        
            for sortfile in sortedFileList2:
              if isfile(sortfile):
                os.remove(sortfile)

            for vfile in splitFileList2 :
              if isfile(vfile):
                os.remove(vfile)
                  
#=========================================================
def Create_SortDbfFileByMultiFields_TestingFiles():
    print("=============Create_SortDbfFileByMultiFields_TestingFiles ========================")
    _dbf = Dbf("Test_MultiField.dbf", new=True)
    _dbf.addField(
        ("CATEGORY", "C", 15),
        ("NAME", "C", 15),
        ("ID", "C", 15),
        ("COST", "N", 8,2),
    )
    
    for (_n, _s, _i, _b) in (
        ("Animal", "cat", "aa", 2.2),
        ("Animal", "cat", "zz", 2.2),
        ("Fruit", "banana", "ee",11.3),
        ("Fruit", "mango", "mm",11.4),
        ("Fruit", "mango", "aa",11.4),
        ("Animal", "cat", "bb", 1.1),
        ("Animal", "cat", "cc", 1.1),
        ("Animal", "cat", "dd", 1.1),
        ("Animal", "elephant","aa",10.3),
        ("Fruit", "orange", "aa",10.5),
        ("Fruit", "orange", "nn",10.6),
        ("Fruit", "orange", "gg",10.7),
        ("Fruit", "orange", "ee",10.3),
        ("Fruit", "apple", "aa",10.3),
        ("Fruit", "apple", "pp",10.3),
        ("Fruit", "apple", "ll",10.3),
        ("Animal", "elephant","bb",10.3),
        ("Animal", "elephant","dd",10.3),
        ("Animal", "elephant","cc",10.3),
        ("Animal", "dog", "bb",10.2),
        ("Animal", "dog", "gg",10.2),
        ("Animal", "dog", "cc",10.2),
        ("Animal", "tiger", "gg",10.4),
        ("Animal", "tiger", "tt",10.4),
        ("Fruit", "orange", "oo",10.5),
        ("Fruit", "orange", "rr",10.5),
        ("Fruit", "apple", "ee",10.3),
        ("Fruit", "banana", "bb",11.3),
        ("Animal", "elephant","kk",10.3),
        ("Animal", "elephant","mm",10.3),
        ("Animal", "dog", "kk",10.2),
        ("Animal", "dog", "ee",10.2),
        ("Fruit", "banana", "aa",11.3),
        ("Fruit", "banana", "nn",11.3),
        ("Fruit", "mango", "gg",11.4),
        ("Fruit", "mango", "oo",11.4),
    ):
        _rec = _dbf.newRecord()
        _rec["CATEGORY"] = _n
        _rec["NAME"] = _s
        _rec["ID"] = _i
        _rec["COST"] = _b
        _rec.store()
    print(repr(_dbf))
    _dbf.close()
#===========================================================================
def Test_SortDbfFileByMultiFields():
     Create_SortDbfFileByMultiFields_TestingFiles()
     SortDbfFileByMultiFields("Test_MultiField.dbf", "CATEGORY")
     SortDbfFileByMultiFields("Test_MultiField.dbf", "CATEGORY","NAME")
     SortDbfFileByMultiFields("Test_MultiField.dbf", "CATEGORY","NAME","ID")
      
#===================================================================================================
def InsertFile(a_thisFilename, a_insertFile, a_AfterRecordNumber) :

    in_dbf = Dbf(a_thisFilename, True)
    in_fieldNameList = in_dbf.fieldNames
    

    TempOutputFile = a_thisFilename.replace(".","_Inserted.")
    out_dbf = Dbf(TempOutputFile, new=True)

    #create the fields
    for f in in_dbf.header.fields:
       out_dbf.addField(f)

    count = 1  # first record contains the field names.
    for in_rec in in_dbf:
       out_rec = out_dbf.newRecord()
       
       for fieldName in in_fieldNameList :
         out_rec[fieldName] = in_rec[fieldName]
       out_rec.store()
       
       count += 1
       if (a_AfterRecordNumber != 0) :
          if (count >= a_AfterRecordNumber) :
              break
    in_dbf.close()
    
    #--- append the insert file -----------------------
    #Assuming the insert file has the same fields
    insert_dbf = Dbf(a_insertFile, True)
    in_fieldNameList2 = insert_dbf.fieldNames
    for insert_rec in insert_dbf:
        out_rec = out_dbf.newRecord()

        for fieldName in in_fieldNameList2 :
           out_rec[fieldName] = insert_rec[fieldName]
        out_rec.store()
    insert_dbf.close()

    #------ copy the rest of the a_thisFilename after a_AfterRecordNumber
    in_dbf = Dbf(a_thisFilename, True)
    in_fieldNameList = in_dbf.fieldNames

    count = 1  # first record contains the field names.
    for in_rec in in_dbf:
     
       count += 1
       if (a_AfterRecordNumber != 0) :
          if (count >= (a_AfterRecordNumber +1)) :
              out_rec = out_dbf.newRecord()
              
              for fieldName in in_fieldNameList :
                 out_rec[fieldName] = in_rec[fieldName]
                 
              out_rec.store()
              
    in_dbf.close()    
    out_dbf.close()
    
    return TempOutputFile
#===========================================================================
def Create_InsertFile1():
    print("=============Create_InsertFile1 ========================")
    _dbf = Dbf("test_insert.dbf", new=True)
    _dbf.addField(
        ("CATEGORY", "C", 15),
        ("NAME", "C", 15),
        ("ID", "C", 15),
        ("COST", "N", 8,2),
    )
    
    for (_n, _s, _i, _b) in (
        ("Animal", "cat", "aa", 2.2),
        ("Animal", "cat", "zz", 2.2),
        ("Animal", "cat", "bb", 1.1),
        ("Animal", "cat", "cc", 1.1),
        ("Animal", "cat", "dd", 1.1),
        ("Animal", "elephant","aa",10.3),
        ("Animal", "elephant","bb",10.3),
        ("Animal", "elephant","dd",10.3),
        ("Animal", "elephant","cc",10.3),
        ("Animal", "dog", "bb",10.2),
        ("Animal", "dog", "gg",10.2),
        ("Animal", "dog", "cc",10.2),
        ("Animal", "tiger", "gg",10.4),
        ("Animal", "tiger", "tt",10.4),
        ("Animal", "elephant","kk",10.3),
    ):
        _rec = _dbf.newRecord()
        _rec["CATEGORY"] = _n
        _rec["NAME"] = _s
        _rec["ID"] = _i
        _rec["COST"] = _b
        _rec.store()
    print(repr(_dbf))
    _dbf.close()
#===========================================================================
def Create_InsertFile2():
    print("=============Create_InsertFile2 ========================")
    _dbf = Dbf("test_insert2.dbf", new=True)
    _dbf.addField(
        ("CATEGORY", "C", 15),
        ("NAME", "C", 15),
        ("ID", "C", 15),
        ("COST", "N", 8,2),
    )
    
    for (_n, _s, _i, _b) in (
        ("Fruit", "banana", "ee",11.3),
        ("Fruit", "mango", "mm",11.4),
        ("Fruit", "mango", "aa",11.4),
        ("Fruit", "orange", "aa",10.5),
        ("Fruit", "orange", "nn",10.6),
        ("Fruit", "orange", "gg",10.7),
        ("Fruit", "orange", "ee",10.3),
        ("Fruit", "apple", "aa",10.3),
        ("Fruit", "apple", "pp",10.3),
        ("Fruit", "apple", "ll",10.3),
        ("Fruit", "orange", "oo",10.5),
        ("Fruit", "orange", "rr",10.5),
        ("Fruit", "apple", "ee",10.3),
        ("Fruit", "banana", "bb",11.3),
        ("Fruit", "banana", "aa",11.3),
    ):
        _rec = _dbf.newRecord()
        _rec["CATEGORY"] = _n
        _rec["NAME"] = _s
        _rec["ID"] = _i
        _rec["COST"] = _b
        _rec.store()
    print(repr(_dbf))
    _dbf.close()

#===========================================================================
def Test_InsertFile():
    
     print(" ==== Test_InsertFile ====")
     Create_InsertFile1()
     Create_InsertFile2()
     InsertFile("test_insert.dbf", "test_insert2.dbf",5)
 #===================================================================================================
def copyDBF(input_filename, output_filename, a_MaxCopyCount = 0):
  
    in_dbf = Dbf(input_filename, True)
    in_fieldNameList = in_dbf.fieldNames
    
    out_dbf = Dbf(output_filename, new=True)
    
    for f in in_dbf.header.fields:
       out_dbf.addField(f)
       
    count = 0
    for in_rec in in_dbf:
       out_rec = out_dbf.newRecord()
       
       for fieldName in in_fieldNameList :
         out_rec[fieldName] = in_rec[fieldName]
       out_rec.store()
       
       count += 1
       if (a_MaxCopyCount != 0) :
          if (count >= a_MaxCopyCount) :
            break
       
    in_dbf.close()
    out_dbf.close()
#===================================================================================================
def SearchAndCreateDBF(input_filename, output_filename, a_searchFieldName, a_searchStringList):
  
    in_dbf = Dbf(input_filename, True)
    in_fieldNameList = in_dbf.fieldNames
    
    out_dbf = Dbf(output_filename, new=True)
    
    for f in in_dbf.header.fields:
       out_dbf.addField(f)
       
    for in_rec in in_dbf:
       
       bFound = False
       for fieldName in in_fieldNameList :
         if fieldName == a_searchFieldName :
             for searchStr in a_searchStringList:
                 if (in_rec[fieldName].find(searchStr) >= 0) :
                     bFound = True
                     break
                   
       if bFound :
             out_rec = out_dbf.newRecord()
            
             for fieldName in in_fieldNameList :
                 out_rec[fieldName] = in_rec[fieldName]
                 #out_rec[fieldName].append(bytes([ord(eachChar)]))
                    
             out_rec.store()

      
    in_dbf.close()
    out_dbf.close()
#===========================================================================
def Create_SearchAndCreateDbf():
    print("=============Create_InsertFile2 ========================")
    _dbf = Dbf("test_SearchAndCreate.dbf", new=True)
    _dbf.addField(
        ("CATEGORY", "C", 15),
        ("NAME", "C", 15),
        ("ID", "C", 15),
        ("COST", "N", 8,2),
    )
    
    for (_n, _s, _i, _b) in (
        ("Fruit", "banana", "ee",11.3),
        ("Fruit", "mango", "mm",11.4),
        ("Fruit", "mango", "aa",11.4),
        ("Fruit", "orange", "aa",10.5),
        ("Fruit", "orange", "nn",10.6),
        ("Fruit", "orange", "gg",10.7),
        ("Fruit", "orange", "ee",10.3),
        ("Fruit", "apple", "aa",10.3),
        ("Fruit", "apple", "pp",10.3),
        ("Fruit", "apple", "ll",10.3),
        ("Fruit", "orange", "oo",10.5),
        ("Fruit", "orange", "rr",10.5),
        ("Fruit", "apple", "ee",10.3),
        ("Fruit", "banana", "bb",11.3),
        ("Fruit", "banana", "aa",11.3),
    ):
        _rec = _dbf.newRecord()
        _rec["CATEGORY"] = _n
        _rec["NAME"] = _s
        _rec["ID"] = _i
        _rec["COST"] = _b
        _rec.store()
    print(repr(_dbf))
    _dbf.close()
#=================================================================================
def test_SearchAndCreateDBF():
    Create_SearchAndCreateDbf()
    a_searchFieldName = "NAME"
    a_searchStringList = ["orange"]
    SearchAndCreateDBF("test_SearchAndCreate.dbf", "test_SearchAndCreate_output.dbf", a_searchFieldName, a_searchStringList)

#=================================================================================
def CreateDBFfromFile(input_filename, new_filename):

    in_dbf = Dbf(input_filename, True)

    new_dbf = Dbf(new_filename, new=True)

    for f in in_dbf.header.fields:
      new_dbf.addField(f)
   
    in_dbf.close()
    return new_dbf
#=================================================================================
def CreateFieldsFromFile(input_filename, new_filename):

    in_dbf = Dbf(input_filename, True)
    #in_fieldNameList = in_dbf.fieldNames

    new_dbf = Dbf(new_filename, new=True)

    for f in in_dbf.header.fields:
      new_dbf.addField(f)
   
    in_dbf.close()
    new_dbf.close()
#=================================================================================
def AppendRecordsToFile(input_filename, append_filename):

    in_dbf = Dbf(input_filename, True)
    in_fieldNameList = in_dbf.fieldNames

    append_dbf = Dbf(append_filename, new=False)

    for in_rec in in_dbf:
       append_rec = append_dbf.newRecord()
       
       for fieldName in in_fieldNameList :
         append_rec[fieldName] = in_rec[fieldName]
       append_rec.store()
  
    in_dbf.close()
    append_dbf.close()
   
#===================================================================================================
def FindRecordsFromFile(a_fileName, a_searchFieldName, a_searchStringList ):
    # a_searchFieldName: Field\Column Name of the database
    # a_searchStringList : Strings to be searched : such as ("aaa","bbb","ccc")
    read_dbf = Dbf(a_fileName, True)
    foundRecords = read_dbf.FindRecords(a_searchFieldName,a_searchStringList)
    read_dbf.close()
    
    return foundRecords
#===================================================================================================
def ReplaceFilesInFileList(dbfFileList, searchField, searchList, replaceStr, replaceFieldList):
  foundfileList = []
  for vfile in dbfFileList :
      print (vfile)
      foundRecords = FindRecordsFromFile(vfile,searchField,searchList)
      if len(foundRecords) > 0 :
          for record in foundRecords:
              print ("      ",record)
          foundfileList.append(vfile)
          
      
  for vfile in foundfileList :
    read_dbf = Dbf(vfile,True)
    read_dbf.FindAndReplaceRecords(searchField, searchList, replaceStr, replaceFieldList)
    read_dbf.close()
#===================================================================================================
def ReplaceFilesInDirectory(searchField, searchList, replaceStr, replaceFieldList,subDirStr = None):

  dbfFileList = getDirectoryFileList(subDirStr,"dbf")
  ReplaceFilesInFileList(dbfFileList,searchField, searchList, replaceStr, replaceFieldList)


#===================================================================================================
def test_dbf_class():

    print("============= test_CreateFieldsFromFile_AppendRecordsToFile ========================")
    test_fileName = "county.dbf"
    dbf = Dbf("test_dbfClass.dbf", new=True)
    dbf.CreateFieldsFromFile(test_fileName)
    dbf.AppendRecordsFromFile(test_fileName)

    oneRecord = ("Hanny", "Cheo", "HC", (1992, 2, 4),10.5)
    dbf.AppendRecord(oneRecord)
    
    dbf.printDbf()
    print (" Total records: ",dbf.GetNumberOfRecords())

    print ("definitions : ", dbf.GetDefinitions())
    dbf.close()
    print("------------------- END ------------------------------------------------------------")
#===================================================================================================
def test_CreateFieldsFromFile_AppendRecordsToFile():
    CreateFieldsFromFile("county.dbf","countyAppend.dbf")
    AppendRecordsToFile("county.dbf","countyAppend.dbf")
#===================================================================================================
def test_FindRecords( ):
    read_dbf = Dbf("Test_fruit.dbf", True)
    searchList = ("orange","apple")
    read_dbf.FindRecords("FRUIT",searchList)
    read_dbf.close()
#===================================================================================================
def test_FindAndReplaceRecords() :
    print ("==== start test_FindAndReplaceRecords ========")
    read_dbf = Dbf("Test_findAndReplace.dbf",True)
    searchField = "FRUIT"
    searchList = ("orange","apple")
    replaceStr = "banana"
    replaceFieldList = [("SIZE","Big"),("COLOUR","Blue")]
    
    read_dbf.FindAndReplaceRecords(searchField, searchList, replaceStr, replaceFieldList)
    read_dbf.close()
    print ("==== end test_FindAndReplaceRecords ========")

#=========================================================
def Test_ReplaceFilesInDirectory():
  # assuming the dbf files have the following structure:
  # Fields ("FRUIT","C",20)
  #        ("SIZE","C",10)
  #        ("SOURCE","C",10)
  #        ("COLOUR","C",10)
  dbfFileList = getDirectoryFileList("TestDir","dbf") # get all the dbf from subdirectory "TestDir"
  foundfileList = []
  searchField = "FRUIT"
  searchList = ("apple","orange","banana")
  
  for vfile in dbfFileList :
      print (vfile)
      foundRecords = FindRecordsFromFile(vfile,searchField,searchList)
      if len(foundRecords) > 0 :
          for record in foundRecords:
              print ("      ",record)
          foundfileList.append(vfile)
          
      
  for vfile in foundfileList :
    read_dbf = Dbf(vfile,True)
    replaceStr = "grape"
    replaceFieldList = [("SIZE","Big")("SOURCE","USA"),("COLOUR","Red")]
    
    read_dbf.FindAndReplaceRecords(searchField, searchList, replaceStr, replaceFieldList)
    read_dbf.close()  
      
#=========================================================
def demoCreateWithBigFieldLength(filename):
    print("=============demoCreate========================")
    _dbf = Dbf(filename, new=True)
    _dbf.addField(
        ("NAME", "C", 216),
    )
      
    for (_n) in (
        ("John"),
        ("Andy"), 
        ("Bill"),
        ("Bobb")
    ):
        _rec = _dbf.newRecord()
        _rec["NAME"] = _n
        _rec.store()
    print(repr(_dbf))
    _dbf.close()
#=========================================================
def MergeFiles(a_mergeFileStr, a_mergeFileList):
    CreateFieldsFromFile(a_mergeFileList[0],a_mergeFileStr)
    for vfile in a_mergeFileList :
        AppendRecordsToFile(vfile, a_mergeFileStr)

#==========================================================================================
#==========  END Of ChiuPikS additions ====================================================
#==========================================================================================


#===================================================================================================
  
  
if (__name__=='__main__'):
    import sys
    _name = len(sys.argv) > 1 and sys.argv[1] or "county.dbf"
    
    #copyDBF("VARIABLE.DBF","VARIABLE1000.DBF",1000)
    #test_dbf_class()
    #demoCreate(_name)
    #demoRead(_name)
    #test_FindRecords()
    #test_FindAndReplaceRecords()
    #test_getDirectoryFileList()
    #test_SortColumn()
    #test_SortColumnAndCreateSortFiles()
    #fileList = ("Sort_1_SortTest.dbf","Sort2_SortTest.dbf","Sort3_SortTest.dbf","Sort4_SortTest.dbf")
    #MergeFiles("MergeSort.dbf",fileList)
    Test_SortDbfFileByMultiFields()
    #Test_SortDbfFileByMultiFields()
    #test_SearchAndCreateDBF()
    #Test_InsertFile()
    print("=============End============================")
    
# vim: set et sw=4 sts=4 :
