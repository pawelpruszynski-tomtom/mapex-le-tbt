"""Pure route-computation functions — no Spark dependency.

Each function takes a dict (row from DataFrame) and a Provider instance,
and returns a plain dict that can be collected into a DataFrame later.
"""

import shapely.wkt

from tbt.navutils.navutils.provider import Provider


def compute_single_provider_route(
    row: dict,
    provider: Provider,
    provider_name: str,
    competitor_name: str,
    sample_id: str,
) -> dict:
    """Compute a single provider route for one origin-destination pair.

    :param row: Dict with keys ``route_id``, ``origin``, ``destination``, ``country``.
    :param provider: Initialised Provider instance.
    :param provider_name: Name written into the output dict.
    :param competitor_name: Name written into the output dict.
    :param sample_id: Sample identifier.
    :return: Dict ready to be turned into a Spark Row.
    """
    origin = shapely.wkt.loads(row["origin"]).coords[0]
    destination = shapely.wkt.loads(row["destination"]).coords[0]

    route = provider.getRoute(origin, destination)
    if route is not None:
        route_time = route.time
        length = route.length
        route_geom = route.geometry
    else:
        route_geom = shapely.wkt.loads("LINESTRING EMPTY")
        route_time = 0.0
        length = 0.0

    return {
        "route_id": row["route_id"],
        "origin": row["origin"],
        "destination": row["destination"],
        "provider_route": route_geom.wkt,
        "provider_route_length": float(length),
        "provider_route_time": float(route_time),
        "provider": provider_name,
        "competitor": competitor_name,
        "country": row["country"],
        "sample_id": sample_id,
    }


def compute_single_competitor_route(
    row: dict,
    provider: Provider,
) -> dict:
    """Compute a single competitor route for one origin-destination pair.

    :param row: Dict with keys ``route_id``, ``origin``, ``destination``.
    :param provider: Initialised Provider (competitor) instance.
    :return: Dict with computed competitor route fields.
    """
    origin = shapely.wkt.loads(row["origin"]).coords[0]
    destination = shapely.wkt.loads(row["destination"]).coords[0]

    route = provider.getRoute(origin, destination)
    if route is not None:
        route_time = route.time
        length = route.length
        route_geom = route.geometry
    else:
        route_geom = shapely.wkt.loads("LINESTRING EMPTY")
        route_time = 0.0
        length = 0.0

    return {
        "route_id": row["route_id"],
        "competitor_route": route_geom.wkt,
        "competitor_route_length": float(length),
        "competitor_route_time": float(route_time),
    }

