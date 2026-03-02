"""Pure RAC (Route Assessment Classification) logic — no Spark dependency."""

import uuid

import shapely.wkt

from tbt.navutils.navutils.provider import Provider
from tbt.navutils.navutils.rac import BUFFER, CriticalSectionIteration


def evaluate_rac_for_route(
    row: dict,
    competitor_provider: Provider,
) -> list[dict]:
    """Evaluate RAC state for a single route.

    Returns a list of dicts (one per critical section, or one for optimal/routing_issue).

    :param row: Dict with keys ``country``, ``provider``, ``route_id``,
                ``provider_route``, ``competitor_route``.
    :param competitor_provider: Initialised Provider for the competitor.
    :return: List of critical section dicts.
    """
    provider_route = shapely.wkt.loads(row["provider_route"])
    competitor_route = shapely.wkt.loads(row["competitor_route"])

    if provider_route.buffer(BUFFER).contains(
        competitor_route
    ) and competitor_route.buffer(BUFFER).contains(provider_route):
        return [
            {
                "country": row["country"],
                "provider": row["provider"],
                "route_id": row["route_id"],
                "case_id": str(uuid.uuid4()),
                "route": row["provider_route"],
                "stretch": "LINESTRING EMPTY",
                "rac_state": "optimal",
            }
        ]

    rac = CriticalSectionIteration(
        provider_route, competitor_provider, competitor_section=competitor_route
    )

    if rac.potential_error:
        return [
            {
                "country": row["country"],
                "provider": row["provider"],
                "route_id": row["route_id"],
                "case_id": str(uuid.uuid4()),
                "route": row["provider_route"],
                "stretch": rac.critical_sections_geometry[i].wkt,
                "rac_state": "potential_error",
            }
            for i in range(len(rac.critical_sections))
        ]

    return [
        {
            "country": row["country"],
            "provider": row["provider"],
            "route_id": row["route_id"],
            "case_id": str(uuid.uuid4()),
            "route": row["provider_route"],
            "stretch": "LINESTRING EMPTY",
            "rac_state": "routing_issue",
        }
    ]
