from datetime import datetime, timedelta
from typing import Any, Dict, List, Union

from aiohttp.hdrs import METH_GET, METH_POST
from dateutil.parser import parse
from pydantic import parse_obj_as
from starlette import status

from teems.nordpoolservice.config import NORDPOOL_CONFIG, get_nordpool_config
from teems.nordpoolservice.exceptions import NordPoolException
from teems.nordpoolservice.model import (
    NordPoolApiTokenResponse,
    NordpoolPrices,
    NordPoolStatus,
    NordPoolTokenResponse,
)
from teems.nordpoolservice.storage import NordPoolTokenStorage

from common.enums.price_area import PriceArea
from common.schemas.price.currency import PriceCurrency
from common.script_helpers.logging import logging
from common.utils.rest_client import RESTClient, RESTClientException

logger = logging.getLogger(__name__)


class NordPoolClient:
    def __init__(self) -> None:
        self._rest_client = RESTClient(key="Nordpool Client")
        self.config = get_nordpool_config()
        self.token_storage: NordPoolTokenStorage = NordPoolTokenStorage()

    async def get_nordpool_token(self) -> NordPoolTokenResponse:
        token_url = str(
            NORDPOOL_CONFIG.base_url / NORDPOOL_CONFIG.routes.connect / NORDPOOL_CONFIG.routes.token
        )
        headers = NORDPOOL_CONFIG.headers
        payload = {
            "scope": "marketdata_api",
            "grant_type": "password",
            "username": NORDPOOL_CONFIG.username,
            "password": NORDPOOL_CONFIG.password,
        }
        response = await self._rest_client.request(
            method=METH_POST, url=token_url, data=payload, headers=headers
        )
        decoded_response_data = await self._rest_client.get_response_data(response=response)
        nordpool_api_token_response = NordPoolApiTokenResponse(**decoded_response_data)
        expires_at = datetime.utcnow() + timedelta(seconds=nordpool_api_token_response.expires_in)
        return_value = NordPoolTokenResponse(
            access_token=nordpool_api_token_response.access_token,
            expires_at=expires_at,
            token_type=nordpool_api_token_response.token_type,
        )
        await self.token_storage.save_token(key=NORDPOOL_CONFIG.token_storage_key, value=return_value)
        return return_value

    async def renew_nordpool_token_if_needed(self) -> NordPoolTokenResponse:
        token_object = await self.token_storage.get_token(key=NORDPOOL_CONFIG.token_storage_key)
        if not token_object or parse(token_object.get("expires_at")) < datetime.utcnow() + timedelta(
            minutes=10
        ):
            return await self.get_nordpool_token()
        return NordPoolTokenResponse(**token_object)

    async def get_prices(
        self,
        delivery_area: PriceArea,
        currency: PriceCurrency,
        start_time: datetime,
        end_time: datetime,
        expected_status: NordPoolStatus = NordPoolStatus.official,
    ) -> NordpoolPrices:
        token = await self.renew_nordpool_token_if_needed()
        day_ahead_url = str(
            NORDPOOL_CONFIG.dayahead_url
            / NORDPOOL_CONFIG.routes.dayahead
            / NORDPOOL_CONFIG.routes.prices
            / NORDPOOL_CONFIG.routes.area,
        )
        request_dict: Dict = dict(
            method=METH_GET,
            url=day_ahead_url,
            headers={
                "Ocp-Apim-Subscription-Key": NORDPOOL_CONFIG.ocp_apim_subscription_key,
                "Authorization": f"Bearer {token.access_token}",
            },
            params={
                "deliveryarea": delivery_area.value,
                "currency": currency.value,
                "startTime": start_time.isoformat(),
                "endTime": end_time.isoformat(),
                "status": expected_status.value,
            },
        )

        response = await self._rest_client.request(**request_dict)
        if response.status not in range(200, 300) or response.status == status.HTTP_204_NO_CONTENT:
            raise NordPoolException(
                "There was an error getting prices from Nordpool with response "
                f"code {response.status} or delivery area: `{delivery_area.value}`"
            )
        try:
            decoded_response_data: Union[
                Dict[str, str], List[Dict[str, Any]]
            ] = await self._rest_client.get_response_data(response=response)
        except RESTClientException as er:
            raise NordPoolException(
                f"Could not decode data for delivery area (PriceArea): `{delivery_area}`"
            ) from er

        prices, *_ = parse_obj_as(List[NordpoolPrices], decoded_response_data)
        return prices
