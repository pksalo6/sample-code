from datetime import datetime, time, timedelta
from typing import Tuple

from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from teems.nordpoolservice.exceptions import NordPoolException
from teems.nordpoolservice.model import NordpoolPrices, NordPoolStatus
from teems.nordpoolservice.nordpool_client import NordPoolClient
from teems.region.abstract import AbstractRegion
from teems.region.factory import get_region_from_price_area
from teems.signal.model import PriceOrigin, price_expiry
from teems.signal.output_models import Prices
from teems.signal.price import BatchOfPrices

from common.enums.price_area import PriceArea
from common.eventbus.events.emitter import emit_event
from common.script_helpers import logging
from common.script_helpers.asyncs import async_main
from common.utils.rest_client import close_rest_client_sessions

logger = logging.getLogger(__name__)


async def _get_prices_for_region(
    start_utc: datetime,
    end_utc: datetime,
    region: AbstractRegion,
    nordpool_status: NordPoolStatus,
) -> NordpoolPrices:
    nordpool_client = NordPoolClient()
    nordpool_prices = await nordpool_client.get_prices(
        delivery_area=region.price_area,
        currency=region.currency,
        start_time=start_utc,
        end_time=end_utc,
        expected_status=nordpool_status,
    )

    plural = "" if len(nordpool_prices.values) == 1 else "s"

    logger.info(
        f"Fetched {len(nordpool_prices.values)} price record{plural} from"
        f" Nordpool for `{region.price_area}` for time range:"
        f" {start_utc} - {end_utc} for `{nordpool_status!s}`",
        extra=dict(
            price_count=len(nordpool_prices.values),
            price_area=region.price_area,
            start_utc=str(start_utc),
            end_utc=str(end_utc),
            status=str(nordpool_status),
        ),
    )
    return nordpool_prices


async def get_batch_of_prices_for_region(
    start_utc: datetime, end_utc: datetime, region: AbstractRegion, nordpool_status: NordPoolStatus
) -> BatchOfPrices:
    try:
        nordpool_prices = await _get_prices_for_region(start_utc, end_utc, region, nordpool_status)
    except NordPoolException:
        return BatchOfPrices(
            intervals=[],
            ts_from=start_utc,
            ts_to=end_utc,
            unit=region.price_unit.for_db,
            region=region,
        )
    else:
        return BatchOfPrices.from_nordpool_prices(nordpool_prices)



async def get_batch_of_prices(
    start_utc: datetime,
    end_utc: datetime,
    region: AbstractRegion,
) -> None:
    official_bop = await get_batch_of_prices_for_region(start_utc, end_utc, region, NordPoolStatus.official)

    await emit_event(
        event=Prices(prices=official_bop, origin=PriceOrigin.nordpool_official, expiry=price_expiry()),
    )

    if official_bop.has_correct_price_count and official_bop.ts_to == end_utc:
        return None

    logger.info(
        "Not enough official prices, getting provisional ones",
        extra=dict(
            price_area=region.price_area,
            start_utc=str(official_bop.ts_from),
            end_utc=str(official_bop.ts_to),
        ),
    )

    provisional_bop = await get_batch_of_prices_for_region(
        start_utc, end_utc, region, NordPoolStatus.provisional
    )

    diff_batch = provisional_bop - official_bop
    await emit_event(
        event=Prices(
            prices=diff_batch,
            origin=PriceOrigin.nordpool_provisional,
            expiry=provisional_price_expiry(time(11, 15)),
        ),
    )

    merged_bop = official_bop.merge(provisional_bop)

    if merged_bop.has_correct_price_count and merged_bop.ts_to == end_utc:
        return None

    logger.warning(
        "Nordpool official and provisional prices are incomplete. Supplementing with existing prices",
        extra=dict(
            price_area=region.price_area,
            start_utc=str(start_utc),
            end_utc=str(end_utc),
        ),
    )

    abomination_bop = await generate_batch_of_prices(start_utc, end_utc, region)
    abomination_bop = abomination_bop - merged_bop

    await emit_event(
        event=Prices(
            prices=abomination_bop,
            origin=PriceOrigin.generated,
            expiry=provisional_price_expiry(time(11, 15)),
        ),
    )


def get_timeframe_for_region(region: AbstractRegion) -> Tuple[datetime, datetime]:

    local_now = region.timezone.local_time_now()
    is_monday = local_now.weekday() == 0
    days_behind_today = 3 if is_monday else 1
    truncated_local_now = local_now.replace(hour=0, minute=0, second=0, microsecond=0)

    start_local = truncated_local_now - timedelta(days=days_behind_today)
    end_local = truncated_local_now + timedelta(days=2)

    start_utc = region.timezone.convert_local_time_to_utc(start_local)
    end_utc = region.timezone.convert_local_time_to_utc(end_local)

    return start_utc, end_utc


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(3),
    retry=retry_if_exception_type(NordPoolException),
)
async def emit_prices_from_nordpool(price_area: PriceArea):  # pragma: no cover
    region = get_region_from_price_area(price_area)
    if not region.is_in_nordpool_service:
        return
    start_utc, end_utc = get_timeframe_for_region(region)
    await get_batch_of_prices(start_utc, end_utc, region)

    logger.info(
        f"Finished loading prices for `{price_area}`",
        extra=dict(
            price_area=region.price_area,
            start_utc=str(start_utc),
            end_utc=str(end_utc),
        ),
    )
