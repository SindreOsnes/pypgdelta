from setuptools import setup, find_packages

setup(
    name='pypgdelta',
    description='Python module for generating a delta script agains an existing database compared to a parsed configration.',
    license="MIT",
    url="https://github.com/SindreOsnes/pypgdelta",
    author='Sindre Osnes',
    author_email='sindre.osnes@gmail.com',
    version='0.0.1',
    include_package_data=True,
    packages=find_packages(exclude=['tests*']),
    install_requires=[
        'psycopg2',
    ],
    test_suite='tests.pypgdelta_tests',

)
