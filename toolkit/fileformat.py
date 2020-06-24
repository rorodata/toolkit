"""
    fileformat
    ~~~~~~~~~~

    This module implements the FileFormat abstractions.
"""
from __future__ import annotations
import csv
import pandas as pd
import numpy as np
import yaml
import re
import datetime
from dataclasses import dataclass, field, asdict, replace as dataclass_replace
from enum import Enum
from typing import Any, List
import logging

logger = logging.getLogger(__name__)

class Datatype(Enum):
    INTEGER = "integer"
    FLOAT = "float"
    STRING = "string"
    DATE = "date"
    DATETIME = "datetime"

@dataclass
class ColumnFormat:
    name: str
    label: str
    datatype: Datatype
    description: str = None
    required: bool = True
    default: Any = None
    regex: str = None
    transform: str = None
    dateformat: str = None
    unique: bool = False
    options: List[str] = None
    missing_values: List[str] = None
    params: dict = field(default_factory=dict)

    def __repr__(self):
        return f"<ColumnFormat: {self.name}>"

    @classmethod
    def get_fields(cls):
        return cls.__dataclass_fields__.keys()

    def process(self, column: pd.Series, report: FileFormatReport) -> pd.Series:
        """Process a column verifying the datatype, constraints and applying
        the transformations specified in this ColumnFormat.
        """
        logger.info("  processing column %s", column.name)
        processors = self.get_processors()
        for p in processors:
            column = p.process(column, report)

        return column

    def get_processors(self):
        if self.unique:
            yield UniquenessProcessor()

        if self.datatype == Datatype.STRING and self.regex:
            yield RegexProcessor(self.regex)

        if self.options is not None:
            yield OptionsProcessor(self.options)

        if self.required:
            yield RequiredProcessor()
        else:
            yield DefaultsProcessor(self.default, self.missing_values)

        # The DatatypeProcessor must the last of all processors.
        # All other processing happens before the datatype conversion.
        yield DatatypeProcessor(self.datatype, dateformat=self.dateformat)

    @classmethod
    def from_dict(cls, d):
        fields = cls.get_fields()
        d1 = {k: v for k, v in d.items() if k in fields}
        # Consider required=False when default is specified
        if 'default' in d1:
            d1.setdefault('required', False)
        if 'datatype' in d1:
            d1['datatype'] = Datatype(d1['datatype'])
        params = {k:v for k, v in d.items() if k not in fields}
        return cls(**d1, params=params)

class UniquenessProcessor:
    """Ensures that all values in a column are unique.
    """
    def process(self, column: pd.Series, report: FileFormatReport) -> pd.Series:
        dups = column[column.duplicated()]
        for i, value in dups.iteritems():
            report.add_row_error(
                error_code='duplicate_value',
                error_message='Found duplicate value: {!r}'.format(value),
                row_index=i,
                column_name=column.name,
                value=value)
        return column

class RegexProcessor:
    """Ensures that all values in a column match the given regular expression.
    """
    def __init__(self, regex):
        self.regex = regex

    def process(self, column: pd.Series, report: FileFormatReport) -> pd.Series:
        rx = re.compile(self.regex)
        for i, value in column.iteritems():
            if value is None or value is np.nan:
                continue
            if not rx.match(value):
                report.add_row_error(
                    error_code='invalid_pattern',
                    error_message='The value is not matching the pattern {}: {!r}'.format(self.regex, value),
                    row_index=i,
                    column_name=column.name,
                    value=value)
        return column

class DatatypeProcessor:
    """Processor to convert the input column of strings into the required type.
    """
    def __init__(self, datatype: Datatype, dateformat: str=None):
        self.datatype = datatype
        self.dateformat_given = dateformat
        self.dateformat = dateformat and self.convert_dateformat(dateformat)

    def process(self, column: pd.Series, report: FileFormatReport) -> pd.Series:
        datatype_info = {
            Datatype.INTEGER: dict(
                dtype="Int64", # Int64 supports NAs, and it is different from np.int64
                missing_value=pd.NA,
                convert=int,
                error_message="Invalid integer: {value!r}"),
            Datatype.FLOAT: dict(
                dtype=np.float32,
                missing_value=np.nan,
                convert=float,
                error_message="Invalid number: {value!r}"),
            Datatype.STRING: dict(
                dtype='str',
                missing_value=None,
                convert=str,
                error_message="Invalid value"),
            Datatype.DATETIME: dict(
                dtype='datetime64[ns]',
                missing_value=np.nan,
                convert=self.to_datetime,
                error_message="Invalid timestamp: {value!r}"),
            Datatype.DATE: dict(
                dtype='str',
                missing_value=None,
                convert=self.to_date,
                error_message="Invalid date: {value!r}"),
        }
        info = datatype_info[self.datatype]
        def convert(i, value):
            if value is pd.NA or value in [None, np.nan, '']:
                return info['missing_value']
            try:
                return info['convert'](value)
            except ValueError:
                report.add_row_error(
                    error_code='invalid-value',
                    error_message=info['error_message'].format(value=value),
                    row_index=i,
                    column_name=column.name,
                    value=value)
                return info['missing_value']

        values = [convert(i, value) for i, value in column.iteritems()]
        return pd.Series(data=values, index=column.index, name=column.name, dtype=info['dtype'])

    def to_datetime(self, value: str) -> datetime.datetime:
        dateformat = self.dateformat
        return value and datetime.datetime.strptime(value, dateformat)

    def to_date(self, value: str) -> str:
        return self.to_datetime(value).date().isoformat()

    def convert_dateformat(self, dateformat):
        """Converts the the dateformat as expected by datetime.
        """
        mapping = {
            ":MM": ":%M", # for minutes
            "DD": "%d",
            "MM": "%m",
            "YYYY": "%Y",
            "YY": "%y",
            "HH": "%H",
            "SS": "%S"
        }
        return re.sub("(HH|:MM|SS|YYYY|YY|MM|DD)",
            lambda m: mapping[m.group()],
            dateformat)

class OptionsProcessor:
    def __init__(self, options):
        self.options = options

    def process(self, column, report) -> pd.Series:
        """Ensures that every value in the column is present the options.
        """
        for i, value in column.iteritems():
            # will be handled by missing-value validator
            if value is None or value == "":
                continue
            if value not in self.options:
                report.add_row_error(
                    error_code='invalid_value',
                    error_message='The value is not one of the allowed options: {!r}'.format(value),
                    row_index=i,
                    column_name=column.name,
                    value=value)
        return column

class RequiredProcessor:
    """Ensures that that the column has no missing values.

    This must be called before converting the datatype of the column.
    """
    def process(self, column, report) -> pd.Series:
        missing_values = ['', np.nan, None]
        missing = column[column.isin(missing_values)]
        for i, value in missing.iteritems():
            report.add_row_error(
                error_code='missing_value',
                error_message='Found missing value: {!r}'.format(value),
                row_index=i,
                column_name=column.name,
                value=value)
        return column

class DefaultsProcessor:
    """Replaces the missing values with the specified default value.
    """
    def __init__(self, default_value, missing_values=None):
        self.default_value = default_value
        self.missing_values = missing_values

    def process(self, column, report) -> pd.Series:
        missing_values = ['', np.nan, None]
        if self.missing_values:
            missing_values += self.missing_values
        return column.replace({v: self.default_value for v in missing_values})

ROW_VALIDATORS = {}

def row_validator(name=None):
    """Registers a row validator with given name.

    Usage:

        @row_validator("something")
        def validate_something(index, row, report) -> List[FileFormatError]:
            ...
    """
    def decorator(func):
        key = name or func.__name__
        ROW_VALIDATORS[key] = func
        return func
    return decorator

class FileFormat:
    """The specifications of a tabular file format.

    The FileFormat specifies the accepted columns with thier name,
    expected column name in the input file, datatype etc.

    The `process_data` method takes a DataFrame created from
    the input file and transforms into a DataFrame in the expected
    format.
    """
    def __init__(self, name, columns, description=None, options=None):
        self.name = name
        self.description = description
        self.columns = columns
        self.options = options or {}
        self.begin_row_index = self.options.get("skiprows", 0) + 1

    def __repr__(self):
        return f"<FileFormat:{self.name}>"

    def get_required_column_names(self):
        return [c.column_name for c in self.columns if c.required]

    def process_file(self, filename) -> FileFormatReport:
        """Process a file according the fileformat specifications.
        """
        logger.info("processing file %r", filename)
        skiprows = self.options.get("skiprows")
        df = self.read_csv(filename, skiprows=skiprows)
        report = self.process_data(df)
        return report.with_filename(filename)

    def read_csv(self, filename, skiprows=0):
        data = list(csv.reader(open(filename, encoding='ascii', errors='ignore')))
        data = data[skiprows:]
        ncols = max(len(row) for row in data)
        columns = data[0]
        d = data[1:]

        # Add names for additional columns
        columns = columns + [f'_x{i}' for i in range(ncols-len(columns))]
        d = [self.resize(ncols, row) for row in d]
        df = pd.DataFrame(d, columns=columns, dtype=str)
        df = df.replace('', np.nan)

        # XXX-Anand: force all the columns to be str
        # Temporary hack until we figure out how to specify the dtupe
        # when creating the dataframe
        for c in df.columns:
            ## XXX-Anand: convert only non-null values to str
            # df[c] = df[c].astype(str)
            df[c] = np.where(pd.isnull(df[c]), df[c], df[c].astype(str))
        return df

    def resize(self, n, row):
        """Fills the row with empty values if the size of row is less than n.
        """
        return row + [''] * (n-len(row))

    def process_data(self, df: pd.DataFrame) -> FileFormatReport:
        """Process given dataframe as per the specifications of the
        this fileformat.

        The df is expected to have the columns with names specified
        in the column_name field of the specifications. This function
        will make sure all the required columns are present and all the
        data validations are matched.

        This function expects that all columns in the df to be string
        columns. This will fail if any of the columns are of numeric type.

        It is recommended to use the process_csv method for processing
        csv files.
        """
        report = FileFormatReport(total_rows=len(df))
        try:
            return self._process(df, report)
        except Exception as e:
            logger.error("Failed with internal error", exc_info=True)
            report.add_file_error("internal_error", str(e))
            return report.with_status(FileStatus.REJECTED)

    def _process(self, df, report):
        expected_column_names = [c.label for c in self.columns]

        if not self.options.get("ignore_additional_columns") and not self.options.get("repeat_last_column"):
            self.check_additional_columns(df, expected_column_names, report)

        self.ensure_expected_columns(df, expected_column_names, report)

        # Always reject the file when there are structural errors
        if report.errors:
            return report.with_status(status=FileStatus.REJECTED)

        column_formats = self.columns
        if self.options.get("repeat_last_column"):
            repeat_format = column_formats[-1]
            column_formats = column_formats[:-1]
        else:
            repeat_format = None

        df2 = pd.DataFrame(index=df.index)
        for c in column_formats:
            df2[c.name] = c.process(df[c.label], report)

        if repeat_format:
            names = df.columns[len(column_formats):]
            columns = [df[name] for name in names]
            df2[repeat_format.name] = self.process_repeat_columns(repeat_format, columns, report)

        if self.options.get("validators"):
            self.run_validators(self.options['validators'], df2, report)

        if report.errors and self.options.get("on_error") == "reject-file":
            return report.with_status(status=FileStatus.REJECTED)

        rejected_rows = {e.row_index for e in report.errors
                        if e.error_level==FileErrorLevel.ROW}
        df2 = df2[~df.index.isin(rejected_rows)]
        return report.with_df(df2)

    def process_repeat_columns(self,
            column_format: ColumnFormat,
            columns: List[pd.Series],
            report: FileFormatReport) -> pd.Series:
        columns = [column_format.process(c, report) for c in columns]
        def notnull(row):
            return [x for x in row if x and x is not np.nan]
        d = [notnull(row) for row in zip(*columns)]
        return pd.Series(d)

    def run_validators(self, validators, df, report):
        for i, row in df.iterrows():
            for v in validators:
                vfunc = ROW_VALIDATORS[v]
                vfunc(i, row, report)

    def check_additional_columns(self, df, expected_column_names, report):
        names = set(expected_column_names)
        extra_columns = [c for c in df.columns if c not in names]
        if extra_columns:
            report.add_file_error(
                error_code='found_unexpected_columms',
                error_message="Found unexpected additional columns: {}".format(", ".join(extra_columns)))

    def ensure_expected_columns(self, df, expected_column_names, report):
        missing_columns = set(expected_column_names)-set(df.columns)
        if missing_columns:
            report.add_file_error(
                error_code='columns_missing',
                error_message='Requied columns missing: {}'.format(", ".join(missing_columns))
            )

    def to_yaml(self, path):
        """Writes this file format into an YAML file.
        """
        d = self.dict()
        with open(path, "w") as f:
            yaml.saf_dump(d, f)

    @classmethod
    def from_file(cls, path):
        """Parses the FileFormat from the specified yaml file.
        """
        d = yaml.safe_load(open(path))
        return cls.from_dict(d)

    @classmethod
    def from_dict(cls, d):
        name = d['name']
        description = d.get('description')
        options = d.get('options')
        columns = [ColumnFormat.from_dict(c) for c in d['columns']]
        return cls(
            name=name,
            columns=columns,
            description=description,
            options=options)

class FileStatus(Enum):
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"

class FileErrorLevel(Enum):
    ROW = "row"
    FILE = "file"

@dataclass
class FileFormatError:
    error_level: FileErrorLevel
    error_code: str
    error_message: str

    # the following fields are present olny when error_level="row"
    row_index: int = None # index starts from 1
    column_name: str = None
    value: str = None

    def dict(self):
        d = asdict(self)
        d['error_level'] = self.error_level.value
        return d

    def __str__(self):
        if self.error_level == FileErrorLevel.FILE:
            return "{}: {}".format(self.error_code, self.error_message)
        else:
            return "[{}#{}] {}:{}".format(self.column_name, self.row_index, self.error_code, self.error_message)

@dataclass
class FileFormatReport:
    """Report of processing a file using a fileformat.
    """
    status: FileStatus = FileStatus.ACCEPTED
    filename: str = None
    df: pd.DataFrame = None
    errors: List[FileFormatError] = field(default_factory=list)
    total_rows: int = 0
    #metadata: Dict[str, object] = field(default_factory=dict)

    def get_rejected_row_count(self):
        return len({e.row_index for e in self.errors if e.row_index is not None})

    @property
    def rejected_row_count(self):
        return len(set(e.row_index for e in self.errors if e.error_level == FileErrorLevel.ROW))

    def __repr__(self):
        return "<Report:status={} #errors={}>".format(self.status, len(self.errors))

    def with_filename(self, filename):
        return dataclass_replace(self, filename=filename)

    def with_status(self, status):
        """Sets the status in a copy of the report and returns it.
        """
        return dataclass_replace(self, status=status)

    def with_df(self, df):
        return dataclass_replace(self, df=df)

    def add_file_error(self, error_code, error_message):
        e = FileFormatError(
            error_level=FileErrorLevel.FILE,
            error_code=error_code,
            error_message=error_message)
        self.errors.append(e)

    def add_row_error(self, error_code, error_message, row_index, column_name, value):
        e = FileFormatError(
            error_level=FileErrorLevel.ROW,
            error_code=error_code,
            error_message=error_message,
            row_index=row_index,
            column_name=column_name,
            value=value)
        self.errors.append(e)
