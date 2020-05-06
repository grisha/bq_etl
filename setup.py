import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='bq_etl',
    version='0.0.2',
    author='Gregory Trubetskoy',
    author_email='grisha@grisha.org',
    description='Simple BigQuery ETL',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/grisha/bq_etl',
    packages=setuptools.find_packages(),
    install_requires=['google-cloud-bigquery >= 1.22.0', 'google-cloud-storage >= 1.23.0'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.0',
)
