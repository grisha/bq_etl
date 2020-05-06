from setuptools import setup, find_packages

setup(
    name='bq_etl',
    version='0.0.1',
    url='https://github.com/grisha/bq_etl.git',
    author='Gregory Trubetskoy',
    author_email='grisha@grisha.org',
    description='Simple BigQuery ETL',
    packages=find_packages(),
    install_requires=['google-cloud-bigquery >= 1.22.0', 'google-cloud-storage >= 1.23.0'],
)
