import asyncio
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import AsyncExitStack, asynccontextmanager

from fastapi import FastAPI
from mcp.server.transport_security import TransportSecuritySettings
from pymongo import AsyncMongoClient
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.envconfig import ClientConfigProfile
from temporalio.worker import Worker

from mcp_server.server import mcp
from ntfy.ntfy_api import Ntfy
from peeweare.peeweare_api import PeeweareAPI
from temporal.activities import DBActivity, NotifyActivity, PeeweareActivity
from temporal.workflows import StartMonitoringWorkflow

from .exceptions import PeeweareAppException
from .routers import lists, monitoring


@asynccontextmanager
async def api_lifespan(app: FastAPI):
    # Initialze Logger
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    )

    # Initialize Peeweare API Client
    app.state.peeweare_api = PeeweareAPI(
        url=os.environ["URL"], headers=json.loads(os.environ["HEADERS"])
    )

    # Initialize Notifier
    app.state.notifier = Ntfy(
        url=os.getenv("NTFY_URL"),
        token=os.getenv("NTFY_TOKEN"),
    )

    # Initialize MongoDB client and database
    mongodb_client = AsyncMongoClient(
        os.environ["MONGODB_HOST"],
        username=os.environ["MONGODB_INITDB_ROOT_USERNAME"],
        password=os.environ["MONGODB_INITDB_ROOT_PASSWORD"],
        authMechanism="SCRAM-SHA-256",
    )
    database = mongodb_client.peeweare
    app.state.monitoring_collection = database.get_collection("monitoring_jobs")
    ping_response = await database.command("ping")
    if int(ping_response["ok"]) != 1:
        logger.error("Problem connecting to database cluster")
        raise PeeweareAppException("Problem connecting to database cluster")
    else:
        logger.info("Connected to database cluster.")

    # Create an index on the expiry_date field to automatically delete expired documents
    await app.state.monitoring_collection.create_index(
        "expiry_date", expireAfterSeconds=0
    )

    # Initialize Temporal Client
    default_profile = ClientConfigProfile.load()
    connect_config = default_profile.to_client_connect_config()
    logger.info(
        f"Address: {connect_config.get('target_host')}, Namespace: {connect_config.get('namespace')}"
    )
    logger.info("Attempting to connect to Temporal client")
    try:
        app.state.temporal_client = await Client.connect(
            **connect_config, data_converter=pydantic_data_converter
        )
        logger.info("Successfully connected to Temporal client")
    except Exception as e:
        logger.error(f"Failed to connect to Temporal client: {e}")

    # Inject Dependencies to Activities
    peeweare_activity = PeeweareActivity(app.state.peeweare_api)
    db_activity = DBActivity(app.state.monitoring_collection)
    notfiy_activity = NotifyActivity(app.state.notifier)

    # Initialize Temporal Worker
    worker = Worker(
        app.state.temporal_client,
        task_queue="peeweare-showtime-notifier-task-queue",
        activities=[
            peeweare_activity.get_movies_showing,
            peeweare_activity.get_movie_showtimes,
            db_activity.save_to_db,
            db_activity.get_from_db,
            db_activity.update_in_db,
            db_activity.delete_from_db,
            notfiy_activity.push_notification,
        ],
        workflows=[StartMonitoringWorkflow],
        activity_executor=ThreadPoolExecutor(5),
    )
    logger.info("Worker is running")
    worker_task = asyncio.create_task(worker.run())
    yield

    # Cleanup
    worker_task.cancel()
    await mongodb_client.close()


@asynccontextmanager
async def mcp_lifespan(app: FastAPI):
    async with mcp.session_manager.run():
        yield


@asynccontextmanager
async def combined_lifespan(app: FastAPI):
    async with AsyncExitStack() as stack:
        await stack.enter_async_context(api_lifespan(app))
        await stack.enter_async_context(mcp_lifespan(app))
        yield


app = FastAPI(
    title="Peeweare Showtime Notifier API with MCP", lifespan=combined_lifespan
)
app.include_router(lists.router)
app.include_router(monitoring.router)

allowed_hosts = json.loads(os.environ.get("ALLOWED_HOSTS", "[]"))
security = TransportSecuritySettings(allowed_hosts=allowed_hosts)
mcp_app = mcp.streamable_http_app(transport_security=security)
app.mount("/", mcp_app)
