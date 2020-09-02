"""Setup for the strct package."""

# !/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import (
    setup,
    find_packages,
)



INSTALL_REQUIRES = []
TEST_REQUIRES = [
    # testing and coverage
    'pytest', 'coverage', 'pytest-cov',
    # non-testing packagesrequired by tests, not by the package
    'sortedcontainers',
    # to be able to run `python setup.py checkdocs`
    'collective.checkdocs', 'pygments',
]


setup(
    name='script',
    description=("Utiltiltes for Pyspakr validation."),
    long_description="Long desc",
    author="Vikas Singh",
    author_email="vikassingh1000@gmail.com",
    version=.1,
    url='https://github.com/vikassingh1000/pyspark_data_validation_utils',
    license="MIT",
    packages=find_packages(exclude=['dist', 'docs', 'tests']),
    python_requires=">=3.5",
    install_requires=INSTALL_REQUIRES,
    extras_require={
        'test': TEST_REQUIRES
    },
    setup_requires=INSTALL_REQUIRES,
    platforms=['any'],
    keywords='python list dict set sortedlist',
    classifiers=[
        # Trove classifiers
        # (https://pypi.python.org/pypi?%3Aaction=list_classifiers)
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.7',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Intended Audience :: Developers',
    ],
)
