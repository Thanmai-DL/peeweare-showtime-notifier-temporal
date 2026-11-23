import json
import logging
from datetime import timedelta

from bson import json_util
from fastapi import APIRouter, Request, status
from temporalio.client import (
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleIntervalSpec,
    ScheduleSpec,
)
from temporalio.common import RetryPolicy

from backend.models import MonitoringRequest, MonitoringRequestWithId, Tags
from temporal.activities import DBActivity
from temporal.workflows import StartMonitoringWorkflow

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/movies/monitoring", tags=[Tags.monitoring])


@router.post(
    "/start",
    summary="Start Monitoring Schedule",
    status_code=status.HTTP_201_CREATED,
)
async def start_monitoring_schedule(
    request: Request,
    body: MonitoringRequest,
) -> MonitoringRequest:
    """
    Start a new monitoring schedule with all the information:

    Args:
        request (MonitoringRequest): The details of the monitoring request, including movie_id, name, theater_id, release_date, and imax.

    Returns:
        MonitoringRequest: The details of the monitoring request, including the schedule ID.
    """
    temporal_client = request.app.state.temporal_client
    timestamp = int(body.release_date.timestamp())
    # Save MonitoringRequest to DB
    document_id = await temporal_client.execute_activity(
        DBActivity.save_to_db,
        args=[body],
        id=f"db-save-{body.movie_id}-{body.theater_id}-{timestamp}",
        task_queue="peeweare-showtime-notifier-task-queue",
        schedule_to_close_timeout=timedelta(seconds=5),
        retry_policy=RetryPolicy(maximum_attempts=2),
    )
    logger.info(
        f"Document for movie_id {body.movie_id} in theater_id {body.theater_id} on {body.release_date.date()} with id {document_id} created in db"
    )
    body = MonitoringRequestWithId(**body.model_dump(), document_id=document_id)
    # Create schedule for MonitoringRequest
    await temporal_client.create_schedule(
        body.schedule_id,
        Schedule(
            action=ScheduleActionStartWorkflow(
                StartMonitoringWorkflow.run,
                body,
                id=f"start-monitoring-workflow-{body.movie_id}-{body.theater_id}-{timestamp}",
                task_queue="peeweare-showtime-notifier-task-queue",
            ),
            spec=ScheduleSpec(
                intervals=[ScheduleIntervalSpec(every=timedelta(minutes=5))],
                end_at=body.expiry_date,
            ),
        ),
    )
    logger.info(
        f"Monitoring schedule created for movie_id {body.movie_id} in theater_id {body.theater_id} on {body.release_date.date()} with id {body.schedule_id}"
    )

    return body


@router.delete(
    "/delete",
    summary="Delete Monitoring Schedule",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_monitoring_schedule(
    request: Request,
    document_id: str,
) -> None:
    """
    Delete a monitoring schedule based on the document ID.

    Args:
        document_id (str): The ID of the document for the movie to stop monitoring.

    Returns:
        None
    """
    temporal_client = request.app.state.temporal_client
    document = await temporal_client.execute_activity(
        DBActivity.get_from_db,
        args=[document_id],
        id=f"get-monitoring-document-{document_id}",
        task_queue="peeweare-showtime-notifier-task-queue",
        schedule_to_close_timeout=timedelta(seconds=5),
        retry_policy=RetryPolicy(maximum_attempts=2),
    )
    if document:
        document = json_util.loads(json.dumps(document))
        handle = temporal_client.get_schedule_handle(
            document["schedule_id"],
        )
        await handle.delete()
        logger.info(
            f"Monitoring schedule deleted for movie_id {document['movie_id']} in theater_id {document['theater_id']} on {document['release_date'].date()} with id {document['schedule_id']}"
        )
        await temporal_client.execute_activity(
            DBActivity.delete_from_db,
            args=[document_id],
            id=f"db-delete-{document['movie_id']}-{document['theater_id']}-{document_id}",
            task_queue="peeweare-showtime-notifier-task-queue",
            schedule_to_close_timeout=timedelta(seconds=5),
            retry_policy=RetryPolicy(maximum_attempts=2),
        )
        logger.info(
            f"Document for movie_id {document['movie_id']} in theater_id {document['theater_id']} on {document['release_date'].date()} with id {document_id} deleted from db"
        )
    else:
        logger.info(f"Document with id {document_id} not found in db")


@router.get(
    "/list",
    summary="List Monitoring Schedules",
)
async def list_monitoring_schedules(
    request: Request,
) -> list[dict]:
    """
    List all monitoring schedules.

    Returns:
        List[dict]: A list of all monitoring schedules in the database.
    """
    db_collection = request.app.state.monitoring_collection
    cursor = db_collection.find({})
    result = []
    async for batch in cursor:
        result.append(json.loads(json_util.dumps(batch)))
    return result
