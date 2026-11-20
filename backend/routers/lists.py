import logging
from datetime import datetime, timedelta

from fastapi import APIRouter, Request
from temporalio.common import RetryPolicy

from backend.models import ShowingType, Tags
from temporal.activities import PeeweareActivity

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/movies/lists", tags=[Tags.lists])


@router.get("/{showing_type}", summary="Lists Movies Showing")
async def get_movies_showing(request: Request, showing_type: ShowingType) -> list[dict]:
    """
    Get a list of movies based on their showing type.

    Args:
        showing_type (ShowingType): The type of showing (nowshowing or comingsoon).

    Returns:
        list[dict]: A list of movies based on their showing type.
    """
    temporal_client = request.app.state.temporal_client
    logger.info(f"Fetching {showing_type} movies from Peeweare API")
    result = await temporal_client.execute_activity(
        PeeweareActivity.get_movies_showing,
        args=[showing_type],
        id=f"get-movies-showing-activity-{int(datetime.now().timestamp())}",
        task_queue="peeweare-showtime-notifier-task-queue",
        schedule_to_close_timeout=timedelta(seconds=5),
        retry_policy=RetryPolicy(maximum_attempts=2),
    )
    return result
