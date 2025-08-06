from setuptools import setup, find_packages

setup(
    name="retail_etl",
    version="0.1",
    packages=find_packages(),
    package_dir={'': 'scripts'},
)