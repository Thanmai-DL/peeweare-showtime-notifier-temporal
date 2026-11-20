import logging
from json import JSONDecodeError

import httpx
from dotenv import load_dotenv

from ntfy.exception import NtfyException
from ntfy.models import NtfyResult

load_dotenv()


class Ntfy:
    def __init__(
        self,
        url: str,
        token: str,
        logger: logging.Logger | None = None,
    ):
        """
        Initialize the Notifications client.

        Args:
            url (str): The URL of the notification service.
            token (str): The authentication token for the notification service.
            logger (logging.Logger): The logger to use.
        """
        self._url = url
        self._token = token
        self._logger = logger or logging.getLogger(__name__)

    async def push(self, payload: dict[str, str]) -> NtfyResult:
        """
        Push a notification.

        Args:
            payload (Dict[str, str]): The notification payload.

        Returns:
            NtfyResult: The result of the notification request.
        """
        http_method = "POST"
        headers = {"Authorization": f"Bearer {self._token}", "Title": payload["title"]}
        log_line_pre = f"method={http_method}, url={self._url}"
        log_line_post = f"{log_line_pre}, success={{}}, status_code={{}}, message={{}}"
        try:
            self._logger.debug(msg=log_line_pre)
            async with httpx.AsyncClient() as client:
                response = await client.request(
                    method=http_method,
                    url=self._url,
                    data=payload.get("message"),
                    headers=headers,
                )
        except httpx.HTTPError as e:
            self._logger.error(msg=(str(e)))
            raise NtfyException("Failed to send notification") from e
        try:
            data_out = response.json()
        except (ValueError, JSONDecodeError) as e:
            self._logger.error(msg=log_line_post.format(False, response.status_code, e))
            raise NtfyException("Bad JSON response") from e
        is_success = 299 >= response.status_code >= 200
        log_line = log_line_post.format(
            is_success, response.status_code, response.reason_phrase
        )
        if is_success:
            self._logger.debug(msg=log_line)
            return NtfyResult(**data_out)
        self._logger.error(msg=log_line)
        raise NtfyException(
            f"Notification failed with status code {response.status_code}: {response.reason_phrase}"
        )
