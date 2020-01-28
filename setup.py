"""
Toolkit
-------

Assorted utilities from Algoshelf.
"""

from setuptools import setup, find_packages
from pathlib import Path
import sys

def get_version():
    """Returns the package version taken from version.py.
    """
    path = Path(__file__).parent / "toolkit" / "version.py"
    code = path.read_text()
    env = {}
    exec(code, env, env)
    return env['__version__']

__version__ = get_version()

install_requires = []
extras_require = {
    'all': ['requests']
}

setup(
    name='algoshelf-toolkit',
    version=__version__,
    author='Algoshelf Team',
    author_email='engineering@algoshelf.net',
    description='Assorted utilities from Algoshelf',
    packages=find_packages(),
    include_package_data=True,
    install_requires=install_requires,
    extras_require=extras_require
)
