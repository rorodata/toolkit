import sys
import click
import json
from .fileformat import FileFormat

@click.command()
@click.option("-f", "--file-format", type=click.Path(exists=True),
    required=True, help="formatting file specifications")
@click.option("-o", "--output_file", help="Path to store the json report")
@click.argument('file', type=click.Path(exists=True))
def validate_fileformat(file_format, file, output_file):
    """Validate the file using the given file_format.
    """
    report = FileFormat.from_file(file_format).process_file(file)
    print(f"File validation status: {report.status.value}\n")
    json_report = json.dumps(report.dict(), indent=4, sort_keys=True)
    if output_file:
        with open(output_file, 'w') as json_file:
            json_file.write(json_report)
    else:
        print(json_report)
    sys.exit(0 if report.is_file_accepted() else 1)
