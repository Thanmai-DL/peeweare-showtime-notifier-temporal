from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from temporal.activities import DBActivity, NotifyActivity, PeeweareActivity
    from temporal.models import MonitoringInputWithId


@workflow.defn(name="StartMonitoringWorkflow")
class StartMonitoringWorkflow:
    @workflow.run
    async def run(self, input: MonitoringInputWithId) -> dict | str:
        showtimes = await workflow.execute_activity_method(
            PeeweareActivity.get_movie_showtimes,
            input,
            activity_id=f"get-movie-showtimes-{input.movie_id}-{input.theater_id}",
            task_queue="peeweare-showtime-notifier-task-queue",
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(maximum_attempts=2),
        )

        if showtimes:
            document = await workflow.execute_activity_method(
                DBActivity.get_from_db,
                input.document_id,
                activity_id=f"get-monitoring-document-{input.movie_id}-{input.theater_id}",
                task_queue="peeweare-showtime-notifier-task-queue",
                start_to_close_timeout=timedelta(seconds=5),
                retry_policy=RetryPolicy(maximum_attempts=2),
            )
            if (
                document
                and document["updates"] > 0
                and document["showtimes"] != showtimes
            ):
                document["showtimes"] = showtimes
                document["updates"] = document["updates"] - 1

                date_obj = input.release_date
                release_date = date_obj.strftime("%d-%m-%Y")
                payload = {
                    "title": f"{'IMAX Shows' if document['imax'] else 'Shows'} updated for {document['name']} on {release_date}",
                    "message": "",
                }
                for experience, shows in document["showtimes"].items():
                    payload["message"] += f"{experience}: "
                    for show in shows:
                        payload["message"] += f"{show}, "
                    payload["message"] = payload["message"].rstrip(", ") + "\n"

                await workflow.execute_activity_method(
                    NotifyActivity.push_notification,
                    payload,
                    activity_id=f"update-monitoring-document-{input.movie_id}-{input.theater_id}",
                    task_queue="peeweare-showtime-notifier-task-queue",
                    start_to_close_timeout=timedelta(seconds=5),
                    retry_policy=RetryPolicy(maximum_attempts=2),
                )

                await workflow.execute_activity_method(
                    DBActivity.update_in_db,
                    document,
                    activity_id=f"update-monitoring-document-{input.movie_id}-{input.theater_id}",
                    task_queue="peeweare-showtime-notifier-task-queue",
                    start_to_close_timeout=timedelta(seconds=5),
                    retry_policy=RetryPolicy(maximum_attempts=2),
                )
