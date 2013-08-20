#!/usr/bin/env python
# coding: utf-8

from setuptools import setup, find_packages


setup(
    name='bredis',
    version='0.1.0',
    description='Redis ORM with async operation support',
    long_description=open('README.md').read().split('\n\n', 1)[1],
    author='Yeolar',
    author_email='yeolar@gmail.com',
    url='http://www.yeolar.com',
    packages=find_packages(),
    install_requires=[
        'tornado==2.4.1',
        'redis>=2.4.10',
        'tornado_redis==2.4.1',
        'python-dateutil==2.1',
    ],
    entry_points={
    },
)
