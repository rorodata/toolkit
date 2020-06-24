from pathlib import Path
import pytest
import pandas as pd
import numpy as np
import yaml
import tempfile
from toolkit.fileformat import (
    FileFormat, Datatype,
    UniquenessProcessor,
    RegexProcessor,
    DatatypeProcessor,
    FileFormatReport,
    OptionsProcessor)

class TestUniquenessProcessor:
    def test_success(self):
        c = pd.Series(['a', 'b', 'c'], name='test')
        p = UniquenessProcessor()
        report = FileFormatReport()
        c2 = p.process(c, report)
        assert c is c2
        assert report.errors == []

    def test_failure(self):
        c = pd.Series(['a', 'b', 'a'], name='test')
        p = UniquenessProcessor()
        report = FileFormatReport()
        c2 = p.process(c, report)
        assert c is c2
        assert len(report.errors) == 1
        e = report.errors[0]
        assert e.row_index == 2
        assert e.error_code == 'duplicate_value'
        assert e.error_message == "Found duplicate value: 'a'"

class TestRegexProcessor:
    def test_success(self):
        c = pd.Series(['FG10001', 'FG2945', 'FG1249'], name='test')
        p = RegexProcessor(regex=r'FG\d+')

        report = FileFormatReport()
        c2 = p.process(c, report)
        assert c is c2
        assert report.errors == []

    def test_failure(self):
        c = pd.Series(['FG10001', 'FG2945', 'X1249'], name='test')
        p = RegexProcessor(regex=r'FG\d+')
        report = FileFormatReport()
        c2 = p.process(c, report)
        assert c is c2
        assert len(report.errors) == 1
        e = report.errors[0]
        assert e.row_index == 2
        assert e.error_code == 'invalid_pattern'
        assert e.error_message == r"The value is not matching the pattern FG\d+: 'X1249'"

class TestOptionsProcessor:
    def test_success(self):
        c = pd.Series(['Yes', 'No', 'Yes'], name='test')
        p = OptionsProcessor(options=['Yes', 'No'])
        report = FileFormatReport()
        c2 = p.process(c, report)
        assert c is c2
        assert report.errors == []

    def test_failure(self):
        c = pd.Series(['Yes', 'No', 'MayBe'], name='test')
        p = OptionsProcessor(options=['Yes', 'No'])
        report = FileFormatReport()
        c2 = p.process(c, report)
        assert c is c2
        assert len(report.errors) == 1
        e = report.errors[0]
        assert e.row_index == 2
        assert e.error_code == 'invalid_value'
        assert e.error_message == "The value is not one of the allowed options: 'MayBe'"

    def test_missing(self):
        c = pd.Series(['Yes', 'No', '', None], name='test')
        p = OptionsProcessor(options=['Yes', 'No'])
        report = FileFormatReport()
        c2 = p.process(c, report)
        assert c is c2
        assert report.errors == []

class TestDatatypeProcessor:
    def test_convert_dateformat(self):
        p = DatatypeProcessor(Datatype.DATETIME)
        assert p.convert_dateformat('YYYY-MM-DD') == '%Y-%m-%d'
        assert p.convert_dateformat('YYYY-MM-DD HH:MM') == '%Y-%m-%d %H:%M'
        assert p.convert_dateformat('YYYY-MM-DD HH:MM:SS') == '%Y-%m-%d %H:%M:%S'
        assert p.convert_dateformat('DD/MM/YYYY') == '%d/%m/%Y'

    def test_integer_success(self):
        c = pd.Series(['1', '2', '3'], name='test')
        p = DatatypeProcessor(Datatype.INTEGER)
        report = FileFormatReport()
        c2 = p.process(c, report)
        assert list(c2) == [1, 2, 3]
        assert report.errors == []

    def test_integer_failure(self):
        c = pd.Series(['1', '2', 'x', '4'], name='test')
        p = DatatypeProcessor(Datatype.INTEGER)
        report = FileFormatReport()
        c2 = p.process(c, report)
        assert list(c2) == [1, 2, pd.NA, 4]
        assert len(report.errors) == 1
        e = report.errors[0]
        assert e.row_index == 2
        assert e.error_code == 'invalid-value'
        assert e.error_message == "Invalid integer: 'x'"

    def test_float_success(self):
        c = pd.Series(['1.1', '2', '3'], name='test')
        p = DatatypeProcessor(Datatype.FLOAT)
        report = FileFormatReport()
        c2 = p.process(c, report)
        expected = pd.Series([1.1, 2.0, 3.0], name="test", dtype="float32")
        assert c2.equals(expected)
        assert report.errors == []

    def test_float_failure(self):
        c = pd.Series(['1.5', '2', 'x', '4'], name='test')
        p = DatatypeProcessor(Datatype.FLOAT)
        report = FileFormatReport()
        c2 = p.process(c, report)
        expected = pd.Series([1.5, 2.0, np.nan, 4.0], name="test", dtype="float32")
        assert c2.equals(expected)
        assert len(report.errors) == 1
        e = report.errors[0]
        assert e.row_index == 2
        assert e.error_code == 'invalid-value'
        assert e.error_message == "Invalid number: 'x'"

    def test_date_success(self):
        c = pd.Series(['10/05/2020', '11/05/2020', '12/05/2020'], name='test')
        p = DatatypeProcessor(Datatype.DATE, dateformat="DD/MM/YYYY")
        report = FileFormatReport()
        c2 = p.process(c, report)
        expected = pd.Series(["2020-05-10", "2020-05-11", "2020-05-12"], name="test", dtype="str")
        assert c2.equals(expected)
        assert report.errors == []

    def test_date_failure(self):
        c = pd.Series(['10/05/2020', '11/05/2020', '12-05-2020'], name='test')
        p = DatatypeProcessor(Datatype.DATE, dateformat="DD/MM/YYYY")
        report = FileFormatReport()
        c2 = p.process(c, report)
        expected = pd.Series(['2020-05-10', '2020-05-11', None], name="test", dtype="str")
        assert c2.equals(expected)
        assert len(report.errors) == 1
        e = report.errors[0]
        assert e.row_index == 2
        assert e.error_code == 'invalid-value'
        assert e.error_message == "Invalid date: '12-05-2020'"

def read_tests_files(path):
    tests = []
    p = Path(__file__).parent.joinpath(path)
    files = p.rglob('*.yml')
    for f in files:
        items = list(yaml.safe_load_all(f.open()))
        items = [dict(item, name="{}: {}".format(f.name, item['name'])) for item in items]
        tests.extend(items)
    return tests

# Get all tests
testdata = read_tests_files("fileformats")
test_ids = [t['name'] for t in testdata]

@pytest.mark.parametrize('testspec', testdata, ids=test_ids)
def test_fileformats(testspec):
    f = FileFormat.from_dict(testspec['fileformat'])
    if 'inputfile_contents' in testspec:
        report = _process_file(f, testspec['inputfile_contents'])
    else:
        report = _process_data(f, testspec['inputfile'])

    if report.errors:
        verify_error(testspec, report.errors)

    # to handle the case of rejecting rows on errors
    if not report.errors or 'result' in testspec:
        verify_result(testspec, report.df)

def _process_data(f, data):
    df = pd.DataFrame(data=data['data'], columns=data['columns'])
    return f.process_data(df)

def _process_file(f, file_contents):
    with tempfile.TemporaryDirectory() as root:
        p = Path(root).joinpath("data.csv")
        p.write_text(file_contents)
        return f.process_file(str(p))

def verify_result(testspec, df):
    response = df.to_dict(orient='split')
    response['data'] = replace_nan(response['data'])
    expected_response = testspec['result']
    assert response['columns'] == expected_response['columns']
    assert response['data'] == expected_response['data']

def replace_nan(data):
    if data is np.nan:
        return None
    elif isinstance(data, list):
        return [replace_nan(x) for x in data]
    else:
        return data

def verify_error(testspec, errors):
    assert testspec.get('errors') == [e.dict() for e in errors]
