from datetime import datetime, timedelta

from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from teems.inventory.db import database_connect, get_engine
from teems.nordpoolservice.exceptions import NordPoolException
from teems.nordpoolservice.model import NordPoolStatus
from teems.nordpoolservice.prices import get_batch_of_prices_for_region
from teems.region.abstract import AbstractRegion
from teems.region.factory import get_region_from_price_area
from teems.signal.model import PriceOrigin, price_expiry
from teems.signal.output_models import Prices
from teems.signal.price import BatchOfPrices

from common.database_handler.compatibility_wrapper import DBConnection
from common.enums.price_area import PriceArea
from common.eventbus.events.emitter import emit_event
from common.script_helpers import logging
from common.script_helpers.asyncs import async_main
from common.utils.rest_client import close_rest_client_sessions

logger = logging.getLogger(__name__)


@async_main(engine=get_engine())
async def main():  # pragma: nocover
    end = datetime.utcnow()
    start = end - timedelta(days=7)
    await update_unofficial_prices(start, end)
    await close_rest_client_sessions()


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(3),
    retry=retry_if_exception_type(NordPoolException),
)
async def _update_unofficial_prices(
    start_utc: datetime,
    end_utc: datetime,
    region: AbstractRegion,
    db_connection: DBConnection = None,
) -> None:
    assert db_connection is not None

    unofficial_bop: BatchOfPrices = await BatchOfPrices.from_db(
        start_utc, end_utc, region, origin_excluded=PriceOrigin.nordpool_official, db_connection=db_connection
    )
    unofficial_bop = unofficial_bop.reduce_to_existing_intervals()
    if unofficial_bop.intervals:
        bop = await get_batch_of_prices_for_region(
            start_utc=unofficial_bop.ts_from,
            end_utc=unofficial_bop.ts_to,
            region=region,
            nordpool_status=NordPoolStatus.official,
        )
        if bop.intervals:
            await emit_event(
                event=Prices(prices=bop, origin=PriceOrigin.nordpool_official, expiry=price_expiry()),
            )