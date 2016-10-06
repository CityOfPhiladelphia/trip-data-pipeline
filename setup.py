from distutils import setup

setup(
    name='phila-taxi-trip-data-pipeline',
    version='1.0.1',
    packages=['phila_taxitrips'],
    scripts=['taxitrips.py'],
    install_requires=[
        'datum',
        'petl',
        'shapely',
        'rtree'
    ],
    dependency_links=[
        'git+https://github.com/CityOfPhiladelphia/trip-data-pipeline.git@master#egg=datum',
    ],
)