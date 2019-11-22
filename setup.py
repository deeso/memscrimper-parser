#!/usr/bin/env python3
from setuptools import setup, find_packages
# configure the setup to install from specific repos and users

DESC = 'memscrimper_parser'
setup(name='memscrimper-parser',
      version='1.0',
      description=DESC,
      author='Adam Pridgen',
      author_email='adam.pridgen.phd@gmail.com',
      install_requires=[],
      packages=find_packages('src'),
      package_dir={'': 'src'},
      dependency_links=[],
)