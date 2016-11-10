from setuptools import setup

setup(
    name='phila-taxi-trip-data-pipeline',
    version='1.0.3',
    packages=['phila_taxitrips'],
    scripts=['taxitrips.py'],
    install_requires=[
        'datum',
        'petl',
        'shapely',
        'rtree',
        'numpy',
    ],
    dependency_links=[
        'https://github.com/CityOfPhiladelphia/datum/tarball/cli#egg=datum',
    ],
)
