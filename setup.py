#! /usr/bin/env python

from distutils.core import setup
from setuptools import find_packages

# pip install -e .                                              # install from source
# python setup.py sdist                                         # create dist
# sudo pip install --no-index --find-links=./dist/ xray         # install dist

setup(
    name='xray',
    version='0',
    packages=find_packages(),
    install_requires=[
        'psutil',
        'gevent', 
	'zmq',
    ],
    license='MIT',
    scripts=['apps/monitor']
)
