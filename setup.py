try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='helper_clickhouse',
    version='0.0.2',
    url='https://github.com/shuge/helper_clickhouse',
    license='MIT License',
    author='Shuge Lee',
    author_email='shuge.lee@gmail.com',
    description='Clickhouse helper',
    long_description="CRUD Clickhouse table, partition etc in pure Python.",
    packages = [
        "helper_clickhouse",
    ],
    #test_suite ="helper_clickhouse.tests",
)
