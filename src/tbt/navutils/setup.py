from setuptools import setup  # pragma: no cover

setup(  # pragma: no cover
    name="navutils",
    version="0.1",
    install_requires=[
        "pytest~=6.2",
        "googlemaps==4.5.3",
        "numpy==1.22.4",
        "polyline==1.4.0",
        "requests==2.25.1",
        "shapely==1.8.0",
        "pandas==1.5.2",
        "sqlalchemy==1.4.45",
        "flexpolyline==0.1.0",
    ]
   
)
