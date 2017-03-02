import os
import socket

"""Setup file for Columbus Worker package."""

import re

try:
    # if setuptools is available, use it to take advantage of its dependency
    # handling
    from setuptools import setup  # pylint: disable=g-import-not-at-top
except ImportError:
    # if setuptools is not available, use distutils (standard library). Users
    # will receive errors for missing packages
    from distutils.core import setup  # pylint: disable=g-import-not-at-top


def get_version():
    with open('colorker/__init__.py') as f:
        return re.findall(r'__version__\s*=\s*\'([.\d]+)\'', f.read())[0]


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


with open('requirements.txt') as f:
    required = f.read().splitlines()

if socket.gethostname() == 'Jo_SpectreX':
    required = []

setup(
    name="columbusworker",
    version=get_version(),
    author="Johnson Kachikaran",
    author_email="johnsoncharles26@gmail.com",
    description=("Columbus Worker Module needed for the distributed execution of workflows in "
                 "Columbus - A Scientific Workflow and Analytics Engine For Spatio-Temporal MultiDimensional Datasets"),
    license="MIT",
    keywords="scientific workflows, distributed, GIS, python, spatio-temporal",
    url="https://github.com/jkachika/columbus-worker",
    packages=['colorker', 'colorker.comm', 'colorker.service'],
    package_data={
        'colorker': [
            'tests/*.py',
        ],
    },
    test_suite='colorker/tests',
    long_description=read('README'),
    classifiers=[
        # Get strings from
        # http://pypi.python.org/pypi?%3Aaction=list_classifiers
        "Development Status :: 2 - Pre-Alpha",
        "Framework :: Django :: 1.9",
        "Intended Audience :: Science/Research",
        'Intended Audience :: Developers',
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2.7",
        "Topic :: Scientific/Engineering :: GIS",
        "Topic :: Scientific/Engineering :: Visualization",
        "Topic :: Software Development :: Libraries :: Python Modules"
    ],
    install_requires=required
)
