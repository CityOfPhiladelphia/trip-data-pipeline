from distutils import setup

setup(
    name='phl-taxi-trip-data-pipeline',
    version='1.0.1',
    packages=['phltaxitrips'],
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