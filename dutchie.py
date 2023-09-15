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
                       "cc2ca38448276ee4b3c03a205511438f0ebcce0fa6db3468be1697630e9e6d96",
                       #"c15335c61b3aa861f8959251f17b6ba5f0a1d5f1d2bdd4c0d691b6bae6f3ceb3",
                       # "f44ceb73f96c7fa3016ea2b9c6b68a5b92a62572317849bc4d6d9a724b93c83d",
                      # "016da194f06fd04ebd72db371c8e2c4a3c4cb81cb6300154526fde47a84f4a4e",
                      # "85813b7a6849a093b56b6d5933d0bb457305a2b296375afd698e4f7ed19ca2b0",
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
                       #"b13689c8a42b9b7405c6ccf92883f31f6119fb5906bf62db3742aaa099e4730b",
			           "093f88f51564d5361b8116f600748d47d9f9d91b675a745fbcef9b7c03d41230",
                       variables)

    return [Box(p) for p in data['filteredProducts']['products']]

async def load(distance=25):
    with requests.Session() as session:
        ds = await dispensary_query(session, distance)
        menus = await asyncio.gather(*[menu_query(session, d.id) for d in ds])
        ps = [p for p in itertools.chain.from_iterable(menus)]
        return ds, ps
