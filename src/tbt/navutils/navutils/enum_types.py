""" Utility types"""

from enum import Enum, auto


class AvoidOptions(Enum):
    """Avoid options

    Each routing API supports a set of avoid options,
    not every avoid option is supported by all the providers.

    Examples::

        >>> prov = Provider.from_properties(name='Genesis')
        >>> # Avoid unpaved roads
        >>> options = RouteOptions(avoid=[AvoidOptions.UNPAVED])
        >>> route = prov.getRoute(origin = [-3.854, 40.304], destination = [-3.842, 40.294], options = options)
        >>> route.geometry.wkt

        >>> # Avoid unpaved roads and motorways
        >>> options = RouteOptions(avoid=[AvoidOptions.UNPAVED, AvoidOptions.MOTORWAY])
        >>> route = prov.getRoute(origin = [-3.854, 40.304], destination = [-3.842, 40.294], options = options)
        >>> route.geometry.wkt
    """

    UNPAVED = auto()
    TOLL = auto()
    MOTORWAY = auto()
    CARPOOL = auto()
    FERRY = auto()
    LEZ = auto()


class RouteType(Enum):
    """Route type (fastest, shortest)

    Examples::

        >>> prov = Provider.from_properties(name='Genesis')
        >>> # Use fastest route
        >>> options = RouteOptions(route_type=RouteType.FASTEST)
        >>> route = prov.getRoute(origin = [-3.854, 40.304], destination = [-3.842, 40.294], options = options)
        >>> route.geometry.wkt
        >>> # Use shortest route
        >>> options = RouteOptions(route_type=RouteType.SHORTEST)
        >>> route = prov.getRoute(origin = [-3.854, 40.304], destination = [-3.842, 40.294], options = options)
        >>> route.geometry.wkt
    """

    FASTEST = auto()
    SHORTEST = auto()


class TravelMode(Enum):
    """Travel Mode

    (car, truck, pedestrian, etc)

    Examples::

        >>> prov = Provider.from_properties(name='Genesis')
        >>> # Use fastest car route
        >>> options = RouteOptions(route_type=RouteType.FASTEST, travel_mode=TravelMode.CAR)
        >>> route = prov.getRoute(origin = [-3.854, 40.304], destination = [-3.842, 40.294], options = options)
        >>> route.geometry.wkt
        >>> # Use shortest bike route
        >>> options = RouteOptions(route_type=RouteType.SHORTEST, travel_mode=TravelMode.BICYCLE)
        >>> route = prov.getRoute(origin = [-3.854, 40.304], destination = [-3.842, 40.294], options = options)
        >>> route.geometry.wkt
    """

    PEDESTRIAN = auto()
    BICYCLE = auto()
    CAR = auto()
    TRUCK = auto()


class SectionType(Enum):
    """Class for section type encapsulation

    With this class, we request these sections to be returned
    by the routing api with its corresponding type.

    Examples::

        >>> prov = Provider.from_properties(name='Genesis')
        >>> # Use fastest car route
        >>> options = RouteOptions(sections=[SectionType.CAR, SectionType.MOTORWAY, SectionType.UNPAVED])
        >>> route = prov.getRoute(origin = [-3.854, 40.304], destination = [-3.842, 40.294], options = options)
        >>> route.geometry.wkt
        >>> # Retrieve the sections
        >>> len(route.sections)
        >>> route.sections[0].geometry.wky
        >>> route.sections[0].section_type
    """

    UNPAVED = auto()
    TOLL = auto()
    CAR = auto()
    TRUCK = auto()
    HOV = auto()
    FERRY = auto()
    CARTRAIN = auto()
    TUNNEL = auto()
    MOTORWAY = auto()
    OTHER = auto()
