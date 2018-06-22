#! /usr/bin/env python

from distutils.core import setup
from setuptools import find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='pyxray',
    version='0.1',
    author='Gregory Freilikhman',
    description='Python client for xrayio tools',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/xrayio/pyxray",
    scripts=['apps/monitor'],
    packages=find_packages(),
    license='MIT',
    classifiers=(
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ),
    install_requires=[
        'zmq',
        'gevent',
        'psutil',
        'FixedList',
    ],
)
