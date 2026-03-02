"""This module contains generic classes to obtain routes from different providers
through a common interface

Examples::

    >>> from tbt.utils.provider import Provider
    >>> prov = Provider.from_properties(name='TT')
    >>> r = prov.getRoute(origin = [29.12852, 40.18264], destination = [29.12599, 40.17819])
    >>> r.geometry.wkt
"""

from .base_provider import BaseProvider
from .bing import BingAPI
from .directions import Directions
from .googleapi import GoogleAPI
from .gt import GT
from .here import HereAPI
from .kakao import Kakao
from .mapbox import MapBox
from .valhalla import Valhalla
from .mmi import MMI
from .zenrin import Zenrin
from .genesysmap import Genesysmap
from .orbis import Orbis

API_KEY_GENESIS =
API_KEY_GOOGLE =
API_KEY_AMIGOALPHA =
API_KEY_HERE =
API_KEY_BING =
API_KEY_KAKAO =
API_KEY_GT =
API_KEY_MMI =
API_KEY_ZENRIN =
API_KEY_GENESYSMAP =


class Provider(BaseProvider):
    """Standard provider class"""

    @staticmethod
    def __parse_api(api, name):
        if api is not None:
            api = api.strip()
            if api == "":
                api = Provider.__infer_api_from_name(name)
        else:
            api = Provider.__infer_api_from_name(name)
        return api

    @staticmethod
    def __parse_endpoint(endpoint, api):
        if endpoint is not None:
            endpoint = endpoint.strip(" /")
            if endpoint == "":
                endpoint = Provider.__infer_endpoint_from_api(api)
            else:
                endpoint = endpoint + "/"
                if not ("http://" in endpoint[:7] or "https://" in endpoint[:8]):
                    endpoint = "http://" + endpoint
        else:
            endpoint = Provider.__infer_endpoint_from_api(api)
        return endpoint

    @staticmethod
    def __infer_api_from_name(name):
        api = ""

        if "TT" in name or "TomTom" in name or "Genesis" in name or "GENESIS" in name:
            api = "Directions"
        elif "STRICT" in name:
            api = "Valhalla-Strict"
        elif "VAL" in name or "-V-" in name or "OSM" in name:
            api = "Valhalla"
        elif "GG" in name or "Google" in name:
            api = "Google"
        elif "OM" in name or "Amigo" in name:
            api = "Amigo-Alpha"
        elif "Orbis" in name or "ORBIS" in name:
            api = "Orbis"
        elif "HERE" in name or "Here" in name:
            api = "Here"
        elif "Bing" in name or "BING" in name:
            api = "Bing"
        elif "Kakao" in name or "KAKAO" in name:
            api = "Kakao"
        elif "GT" in name or "gt" in name:
            api = "GT"
        elif "mapbox" in name.lower() or "mb" in name.lower():
            api = "MapBox"
        elif "mmi" in name.lower() or "mapmyindia" in name.lower():
            api = "MMI"
        elif "zenrin" in name.lower() :
            api = "Zenrin"
        elif "genesysmap" in name.lower() :
            api = "Genesysmap"
        return api

    @staticmethod
    def __infer_endpoint_from_api(api):
        endpoint = "http://0.0.0.0/"

        if api == "Directions":
            endpoint = "https://api.tomtom.com/routing/1/calculateRoute/"
        elif api == "Amigo-Alpha":
            endpoint = "https://api.tomtom.com/maps/orbis/routing/calculateRoute/"
        elif api == "Orbis":
            endpoint = "https://api.tomtom.com/maps/orbis/routing/calculateRoute/"
        elif api in ("Valhalla", "Valhalla-Strict"):
            endpoint = "http://tt-valhalla-api.maps-orbis-analytics-dev.orbis.az.tt3.com:8002/"
        elif api == "Google":
            endpoint = "http://google/"
        elif api == "Here":
            endpoint = "https://router.hereapi.com/v8/"
        elif api == "Bing":
            endpoint = "https://dev.virtualearth.net/REST/v1/Routes/"
        elif api == "Kakao":
            endpoint = "https://apis-navi.kakaomobility.com/v1/directions"
        elif api == "GT":
            endpoint = "http://api-route-pre.mapfan.com/v1/calcroute"
        elif api == "MapBox":
            endpoint = "https://api.mapbox.com/directions/v5/mapbox/"
        elif api == "MMI":
            endpoint = "https://apis.mappls.com/advancedmaps/v1/"
        elif api == "Zenrin":
            endpoint = "https://test-web.zmaps-api.com/route/route_mbn/drive_ptp"
        elif api == "Genesysmap":
            endpoint = "https://mapapi.genesysmap.com/api/v1/directions/route"
        return endpoint

    @staticmethod
    def from_properties(
        name,
        product="",
        url="",
        api="",
        endpoint="",
        qps_limit=3,
        gg_api_count: int = 0,
        directions_api_count: int = 0,
        valhalla_api_count: int = 0,
        here_api_count: int = 0,
        bing_api_count: int = 0,
        kakao_api_count: int = 0,
        gt_api_count: int = 0,
        mapbox_api_count: int = 0,
        mmi_api_count: int = 0,
        zenrin_api_count: int = 0,
        genesysmap_api_count: int = 0,
        orbis_api_count: int = 0,
    ):
        """Creates a new instance of Provider

        :param name: Provider name. Some accepted values are 'TT', 'GG', 'OM', 'OSM',
         'OSM-STRICT', 'OM-VAL', 'OM_CUSTOM', 'HERE'
        :type name: str
        :param product: Product, e.g. 2022.09.012, defaults to ''
        :type product: str, optional
        :param url: (deprecated - same as endpoint), defaults to ''
        :type url: str, optional
        :param api: API name. One of 'Directions', 'Valhalla', 'Valhalla-Strict', 'Google',
         inferred from provider name if omitted, defaults to ''
        :type api: str, optional
        :param endpoint: http or https api endpoint of the provider (if exists),
         defaults to '' (inferred from api if omitted)
        :type endpoint: str, optional
        :param qps_limit: Limit of queries per second of the API endpoint.
         Uses a `time.sleep` to approximately match the qps, defaults to 3
        :type qps_limit: int, optional
        :raises Exception: if endpoint or api information cannot be inferred from name
        :return: Provider to retrieve routes.
        :rtype: Provider
        """
        name = name.strip()
        api = Provider.__parse_api(api, name)

        if endpoint == "" or endpoint is None:
            endpoint = url

        endpoint = Provider.__parse_endpoint(endpoint, api)

        if api in ["Directions"]:
            return Directions(
                name,
                api,
                endpoint,
                API_KEY_GENESIS,
                qps_limit,
                product,
                gg_api_count,
                directions_api_count,
                valhalla_api_count,
                here_api_count,
                bing_api_count,
                kakao_api_count,
                gt_api_count,
                mapbox_api_count,
                mmi_api_count,
                zenrin_api_count,
                genesysmap_api_count,
                orbis_api_count,
            )

        if api in ["Amigo-Alpha"]:
            return Directions(
                name,
                api,
                endpoint,
                API_KEY_AMIGOALPHA,
                qps_limit,
                product,
                gg_api_count,
                directions_api_count,
                valhalla_api_count,
                here_api_count,
                bing_api_count,
                kakao_api_count,
                gt_api_count,
                mapbox_api_count,
                mmi_api_count,
                zenrin_api_count,
                genesysmap_api_count,
                orbis_api_count,
            )

        if api in ["Orbis"]:
            return Orbis(
                name,
                api,
                endpoint,
                API_KEY_AMIGOALPHA,
                qps_limit,
                product,
                gg_api_count,
                directions_api_count,
                valhalla_api_count,
                here_api_count,
                bing_api_count,
                kakao_api_count,
                gt_api_count,
                mapbox_api_count,
                mmi_api_count,
                zenrin_api_count,
                genesysmap_api_count,
                orbis_api_count,
            )

        if api in ["Valhalla", "Valhalla-Strict"]:
            return Valhalla(
                name,
                api,
                endpoint,
                "",
                qps_limit,
                product,
                gg_api_count,
                directions_api_count,
                valhalla_api_count,
                here_api_count,
                bing_api_count,
                kakao_api_count,
                gt_api_count,
                mapbox_api_count,
                mmi_api_count,
                zenrin_api_count,
                genesysmap_api_count,
                orbis_api_count,
            )

        if api in ["Google"]:
            return GoogleAPI(
                name,
                api,
                endpoint,
                API_KEY_GOOGLE,
                qps_limit,
                product,
                gg_api_count,
                directions_api_count,
                valhalla_api_count,
                here_api_count,
                bing_api_count,
                kakao_api_count,
                gt_api_count,
                mapbox_api_count,
                mmi_api_count,
                zenrin_api_count,
                genesysmap_api_count,
                orbis_api_count,
            )

        if api in ["Here"]:
            return HereAPI(
                name,
                api,
                endpoint,
                API_KEY_HERE,
                qps_limit,
                product,
                gg_api_count,
                directions_api_count,
                valhalla_api_count,
                here_api_count,
                bing_api_count,
                kakao_api_count,
                gt_api_count,
                mapbox_api_count,
                mmi_api_count,
                zenrin_api_count,
                genesysmap_api_count,
                orbis_api_count,
            )

        if api in ["Bing"]:
            return BingAPI(
                name,
                api,
                endpoint,
                API_KEY_BING,
                qps_limit,
                product,
                gg_api_count,
                directions_api_count,
                valhalla_api_count,
                here_api_count,
                bing_api_count,
                kakao_api_count,
                gt_api_count,
                mapbox_api_count,
                mmi_api_count,
                zenrin_api_count,
                genesysmap_api_count,
                orbis_api_count,
            )

        if api in ["Kakao"]:
            return Kakao(
                name,
                api,
                endpoint,
                API_KEY_KAKAO,
                qps_limit,
                product,
                gg_api_count,
                directions_api_count,
                valhalla_api_count,
                here_api_count,
                bing_api_count,
                kakao_api_count,
                gt_api_count,
                mapbox_api_count,
                mmi_api_count,
                zenrin_api_count,
                genesysmap_api_count,
                orbis_api_count,
            )

        if api in ["GT"]:
            return GT(
                name,
                api,
                endpoint,
                API_KEY_GT,
                qps_limit,
                product,
                gg_api_count,
                directions_api_count,
                valhalla_api_count,
                here_api_count,
                bing_api_count,
                kakao_api_count,
                gt_api_count,
                mapbox_api_count,
                mmi_api_count,
                zenrin_api_count,
                genesysmap_api_count,
                orbis_api_count,
            )

        if api in ["MapBox"]:
            return MapBox(
                name,
                api,
                endpoint,
                API_KEY_MAPBOX,
                qps_limit,
                product,
                gg_api_count,
                directions_api_count,
                valhalla_api_count,
                here_api_count,
                bing_api_count,
                kakao_api_count,
                gt_api_count,
                mapbox_api_count,
                mmi_api_count,
                orbis_api_count,
            )

        if api in ["MMI"]:
            return MMI(
                name,
                api,
                endpoint,
                API_KEY_MMI,
                qps_limit,
                product,
                gg_api_count,
                directions_api_count,
                valhalla_api_count,
                here_api_count,
                bing_api_count,
                kakao_api_count,
                gt_api_count,
                mapbox_api_count,
                mmi_api_count,
                zenrin_api_count,
                genesysmap_api_count,
                orbis_api_count,
            )
        if api in ["Zenrin"]:
            return Zenrin(
                name,
                api,
                endpoint,
                API_KEY_ZENRIN,
                qps_limit,
                product,
                gg_api_count,
                directions_api_count,
                valhalla_api_count,
                here_api_count,
                bing_api_count,
                kakao_api_count,
                gt_api_count,
                mapbox_api_count,
                zenrin_api_count,
                genesysmap_api_count,
                orbis_api_count,
            )
        if api in ["Genesysmap"]:
            return Genesysmap(
                name,
                api,
                endpoint,
                API_KEY_GENESYSMAP,
                qps_limit,
                product,
                gg_api_count,
                directions_api_count,
                valhalla_api_count,
                here_api_count,
                bing_api_count,
                kakao_api_count,
                gt_api_count,
                mapbox_api_count,
                zenrin_api_count,
                genesysmap_api_count,
                orbis_api_count,
            )
        raise ValueError("Provider unknown")
