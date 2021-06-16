from setuptools import setup, find_packages

setup(
    name="binlog2s3",
    version="1.0",
    description="Stream MySQL binary logs to s3",
    author="Peter Boros",
    author_email="peter.boros@percona.com",
    packages=find_packages(),
    install_requires=['boto3'],
    entry_points={
        'console_scripts': ['binlog2s3=binlog2s3.__main__:main']
    }
)