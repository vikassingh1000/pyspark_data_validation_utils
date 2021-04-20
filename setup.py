import os

from setuptools import setup


# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="pyspark_data_validation_utils",
    version="0.0.21",
    author="Vikas Singh",
    author_email="vikassingh1000@gmail.com",
    description="Its Data cleansing tool, meant for Pyspark projects. ",
    license="APACHE",
    keywords="Pyspark",
    url="https://github.com/vikassingh1000/pyspark_data_validation_utils",
    packages=["pyspark_data_validation_utils"],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: BSD License",
    ],
)
