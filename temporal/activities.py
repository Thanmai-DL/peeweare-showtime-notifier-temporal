import json

from bson import ObjectId, json_util
from temporalio import activity

from .models import MonitoringInput


class PeeweareActivity:
    def __init__(self, api_client):
        self.api_client = api_client

    @activity.defn
    async def get_movies_showing(self, showing_type: str) -> list[dict]:
        if showing_type == "nowshowing":
            activity.logger.info("Fetching now showing movies from Peeweare API")
            return await self.api_client.nowshowing()
        elif showing_type == "comingsoon":
            activity.logger.info("Fetching coming soon movies from Peeweare API")
            return await self.api_client.comingsoon()
        else:
            activity.logger.error(
                f"Invalid showing type: {showing_type}. Must be 'nowshowing' or 'comingsoon'."
            )
            raise ValueError(
                f"Invalid showing type: {showing_type}. Must be 'nowshowing' or 'comingsoon'."
            )

    @activity.defn
    async def get_movie_showtimes(self, input: MonitoringInput) -> dict | None:

        activity.logger.info(
            f"Fetching showtimes for movie_id {input.movie_id} in theater_id {input.theater_id} on {input.release_date.date()} from Peeweare API"
        )
        result = await self.api_client.showtimes(
            input.movie_id, input.theater_id, str(input.release_date.date())
        )
        if result is None:
            activity.logger.info(
                f"Showtimes for movie_id {input.movie_id} in theater_id {input.theater_id} on {input.release_date.date()} not yet available"
            )
            return None
        if input.imax:
            shows = {k: v for k, v in result.shows.items() if "IMAX" in k}
            return shows
        return result.shows


class DBActivity:
    def __init__(self, db_collection):
        self.db_collection = db_collection

    @activity.defn
    async def save_to_db(self, document: MonitoringInput) -> str:
        result = await self.db_collection.insert_one(document.model_dump())
        activity.logger.info(f"Saved {document} to the database")
        return str(result.inserted_id)

    @activity.defn
    async def get_from_db(self, document_id: str) -> dict | None:
        if ObjectId.is_valid(document_id):
            activity.logger.info(
                f"Fetching item from the database with query: '_id': {document_id}"
            )
            result = await self.db_collection.find_one({"_id": ObjectId(document_id)})
            activity.logger.info(
                f"Retrieved document from the database with id {document_id}"
            )
            return json.loads(json_util.dumps(result))
        else:
            activity.logger.error(f"Invalid document id: {document_id}")

    @activity.defn
    async def update_in_db(self, document: dict) -> str:
        document = json_util.loads(json.dumps(document))
        result = await self.db_collection.update_one(
            {"_id": document["_id"]}, {"$set": document}
        )
        activity.logger.info(
            f"Updated document in the database with id {document['_id']!s}"
        )
        return str(result.upserted_id)

    @activity.defn
    async def delete_from_db(self, document_id: str) -> None:
        if ObjectId.is_valid(document_id):
            result = await self.db_collection.delete_one({"_id": ObjectId(document_id)})
            activity.logger.info(
                f"Deleted {result.deleted_count} document in the database with id {document_id}"
            )
            return
        else:
            activity.logger.warning(f"Invalid document id: {document_id}")


class NotifyActivity:
    def __init__(self, api_client):
        self.api_client = api_client

    @activity.defn
    async def push_notification(self, payload: dict) -> dict:
        activity.logger.info(
            f"Sending push notification with message: {payload['message']}"
        )
        return await self.api_client.push(payload)
