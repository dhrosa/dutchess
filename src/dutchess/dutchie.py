import json
import aiolimiter
from box import Box
from loguru import logger
import requests
import asyncio
import itertools

logger.disable(__name__)
rate_limiter = aiolimiter.AsyncLimiter(10, 1)

async def query(*args, **kwargs):
    async with rate_limiter:
        return await raw_query(*args, **kwargs)

async def raw_query(session, op_name, query_hash, variables):
    extensions = dict(persistedQuery=dict(
        version=1, sha256Hash=query_hash))
    params = dict(
        operationName=op_name,
        variables=json.dumps(variables),
        extensions=json.dumps(extensions))

    request = requests.Request("GET", "https://dutchie.com/graphql", params=params).prepare()
    logger.debug("Fetching from upstream.", query_params=params, url=request.url)
    request.headers["Content-Type"] = "application/json"
    response = await asyncio.to_thread(session.send, request)
    logger.debug("Response", elapsed=response.elapsed.total_seconds(), size=len(response.content))
    try:
        response.raise_for_status()
    except Exception as e:
        e.add_note(response.content.decode('utf-8'))
        raise
    raw_data = response.json()
    if 'data' not in raw_data:
        raise ValueError(raw_data)
    data = raw_data.pop('data')
    if raw_data:
        logger.debug("Unconsumed response fields. ", raw_data=raw_data)
    return data

async def dispensary_query(session, distance):
    variables = dict(
        dispensaryFilter=dict(
            medical=True,
            recreational=False,
            sortBy="distance",
            activeOnly=True,
            city="Somerville",
            country = "United States",
            nearLat = 42.387597,
            nearLng = -71.099497,
            destinationTaxState = "MA",
            distance = distance,
            openNowForPickup = False,
            acceptsCreditCardsPickup = False,
            acceptsDutchiePay = False,
            offerCurbsidePickup = False,
            offerPickup = True))

    data = await query(session,
                       "ConsumerDispensaries",
                       "10f05353272bab0f3ceeff818feef0a05745241316be3a5eb9f3e66ca927a40d",
                       variables)
    return [Box(d) for d in data['filteredDispensaries']]


async def menu_query(session, dispensary_id):
    variables = dict(
        includeEnterpriseSpecials = False,
        includeTerpenes = True,
        includeCannabinoids = True,
        productsFilter = dict(
            dispensaryId = dispensary_id,
            pricingType = "med",
            Status = "Active",
            strainTypes = [],
            types = ["Flower"],
            bypassOnlineThresholds = False,
            isKioskMenu = False,
            removeProductsBelowOptionThresholds = True))

    data = await query(session,
                       "IndividualFilteredProduct",
                       "63df54bf308c5176e422805961f73ebdda75ae3a60f9885831be146b4a7cb32e",
                       variables)

    return [Box(p) for p in data['filteredProducts']['products']]

async def load(distance=25):
    with requests.Session() as session:
        ds = await dispensary_query(session, distance)
        menus = await asyncio.gather(*[menu_query(session, d.id) for d in ds])
        ps = [p for p in itertools.chain.from_iterable(menus)]
        return ds, ps
