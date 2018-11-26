import os

from setuptools import find_packages, setup

here = os.path.abspath(os.path.dirname(__file__))

setup(
    name='earth',
    version='0.0.1',
    description='Time series data-store mainly focused finanical data',
    url='https://github.com/dorukcan/earth',
    author='Dorukcan Kişin',
    author_email='dckisin@gmail.com',
    keywords='time series datastore',

    packages=find_packages(
        include=('earth', 'earth.*'),
    ),

    install_requires=[
        'attrs', 'psycopg2', 'tqdm', 'python-slugify', 'lxml', 'bs4'
    ],

    project_urls={
        'Bug Reports': 'https://github.com/dorukcan/earth/issues',
        'Source': 'https://github.com/earth/venus/',
    },
)
