import asyncio
import logging
import pathlib
import threading
from contextlib import asynccontextmanager
from typing import Any, Dict

import uvicorn
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from config import UvicornServerSettings, settings
from models import WSTopic, WSTopicAction
from report.job_models import (
    JobStateUpdate,
    RestartJobBatchResponse,
    RestartJobRequest,
    RestartJobResponse,
)
from report.state_manager import state_manager
from report.ws_manager import WSConnectionManager

logger = logging.getLogger("WebScraper")


@asynccontextmanager
async def lifespan(app: FastAPI):
    update_interval_s = settings.update_interval_ms / 1000.0
    logger.info(
        f"Starting background task to broadcast job updates every {settings.update_interval_ms} ms."
    )
    app.state.background_task = asyncio.create_task(
        update_broadcaster(update_interval_s), name="bcast_update"
    )
    try:
        yield
    finally:
        bg_task_ws_update = app.state.background_task
        if bg_task_ws_update:
            logger.info("Cancelling background task.")
            bg_task_ws_update.cancel()
            try:
                await bg_task_ws_update
            except asyncio.CancelledError:
                logger.info("Background task successfully cancelled.")


async def update_broadcaster(interval_s: float):
    """
    Periodically fetches job statuses and broadcasts them to clients
    subscribed to the 'all_jobs' topic.

    Args:
        interval_s: The time to wait between broadcasts.
    """
    while True:
        try:
            await asyncio.sleep(interval_s)
            await wsconnection_manager.broadcast_to_topic(
                WSTopic.ALL_JOBS,
                JobStateUpdate(
                    jobData=await state_manager.get_all_job_statuses(json_encoded=True)
                ).model_dump(),
            )
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("Error in background update broadcaster:")


app = FastAPI(
    title="Web Scraper Admin Panel",
    description="Manage and monitor scraping tasks, schedules, and logs.",
    version="1.0.0",
    summary="Admin backend for a web scraping platform.",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    debug=settings.debug,
    lifespan=lifespan,
)
app.mount(
    "/static",
    StaticFiles(directory=pathlib.Path(__file__).parent / "static"),
    name="static",
)
templates = Jinja2Templates(directory=pathlib.Path(__file__).parent / "index")
wsconnection_manager = WSConnectionManager()


@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """Serve the admin dashboard HTML from file."""
    try:
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "ws_max_retries_on_disconnect": settings.server.ws_max_retries_on_disconnect,
                "ws_reconnect_interval_ms": settings.server.ws_reconnect_interval_ms,
            },
        )
    except Exception as e:
        return HTMLResponse(
            content=f"<h1>Error loading dashboard</h1><p>{str(e)}</p>", status_code=500
        )


@app.websocket("/ws/jobs", name="All updated job statuses")
async def ws_jobs(websocket: WebSocket):
    """
    WebSocket endpoint for real-time job status updates.
    - A new client receives the full current job list upon connection.
    - The connection is kept alive to receive subsequent broadcasted updates.
    """
    await wsconnection_manager.connect(websocket)
    try:
        logger.info(f"Sending initial state to client {websocket.client}")
        await websocket.send_json(
            JobStateUpdate(
                jobData=await state_manager.get_all_job_statuses(json_encoded=True)
            ).model_dump(),
            mode="text",
        )

        while True:
            message = await websocket.receive_json()
            action = WSTopicAction.from_str(message.get("action"))
            topic = WSTopic.from_str(message.get("topic"))

            if topic != WSTopic.UNKNOWN_TOPIC:
                if action == WSTopicAction.SUBSCRIBE:
                    await wsconnection_manager.subscribe(websocket, topic)
                elif action == WSTopicAction.UNSUBSCRIBE:
                    await wsconnection_manager.unsubscribe(websocket, topic)
                else:
                    logger.warning(
                        f"Received unknown action from {websocket.client}: {action.value}"
                    )
            else:
                logger.warning(
                    f"Received unknown topic from {websocket.client}: {topic.value}"
                )

    except WebSocketDisconnect:
        logger.info(f"Client {websocket.client} disconnected gracefully.")
    except Exception as e:
        # Catch other potential exceptions during the connection lifetime
        logger.error(
            f"An unexpected error occurred with client {websocket.client}: {e}",
            exc_info=True,
        )
    finally:
        await wsconnection_manager.disconnect(websocket)


@app.get("/api/jobs", deprecated=True)
async def get_jobs_status():
    """API endpoint to get the current status of all jobs."""
    return await state_manager.get_all_job_statuses()


@app.post("/api/jobs/restart", response_model=RestartJobResponse)
async def restart_failed_job(payload: RestartJobRequest):
    """
    Restarts a job that is in a 'Failed' or 'Permanently Failed' state.
    """
    restart_response: RestartJobResponse = await state_manager.restart_job(
        job_id=payload.jobId
    )
    if not restart_response.success:
        logger.warning(f"Failed to restart job {payload.jobId}, job remains [failed].")
        if "not found" in restart_response.message:
            raise HTTPException(status_code=404, detail=restart_response.message)
        else:
            raise HTTPException(status_code=409, detail=restart_response.message)

    logger.info(f"Job {payload.jobId} restarted.")

    await wsconnection_manager.broadcast_to_topic(
        WSTopic.ALL_JOBS,
        JobStateUpdate(
            jobData=await state_manager.get_all_job_statuses(json_encoded=True)
        ).model_dump(),
    )

    return restart_response


@app.post("/api/jobs/restart_all", response_model=RestartJobBatchResponse)
async def restart_all_permanently_failed_jobs():
    """
    Restarts all jobs currently in the 'Permanently Failed' state.
    """
    try:
        result = await state_manager.restart_all_failed_jobs()
        logger.info(
            f"{result.restarted_count}/{result.restarted_count+result.failed_count} jobs scheduled for restart."
        )
        await wsconnection_manager.broadcast_to_topic(
            WSTopic.ALL_JOBS,
            JobStateUpdate(
                jobData=await state_manager.get_all_job_statuses(json_encoded=True)
            ).model_dump(),
        )

        return result
    except Exception as e:
        logger.error(
            f"An unexpected error occurred during bulk restart: {e}", exc_info=True
        )
        raise HTTPException(
            status_code=500,
            detail="An internal server error occurred while restarting jobs.",
        )


def start_web_server():
    """Starts the FastAPI web server in a separate thread."""
    uvicorn_settings: UvicornServerSettings = settings.server
    if uvicorn_settings.workers is not None and uvicorn_settings.workers > 1:
        logger.warning(
            "Uvicorn 'workers' parameter is set to > 1. "
            "When running Uvicorn programmatically in a separate Python `threading.Thread`, "
            "it's strongly recommended to use `workers=1` to avoid multiprocessing conflicts "
            "and lifecycle management issues. "
        )
        raise ValueError(
            "Cannot start Uvicorn with workers > 1 in a separate thread. Adjust settings or use direct Uvicorn launch."
        )

    uvicorn_config_params: Dict[str, Any] = dict(
        app=app,
        host=uvicorn_settings.host_str,
        port=uvicorn_settings.port,
        log_level=uvicorn_settings.log_level,
        timeout_graceful_shutdown=uvicorn_settings.timeout_graceful_shutdown,
        limit_concurrency=uvicorn_settings.limit_concurrency,
        reload=uvicorn_settings.reload,
        workers=uvicorn_settings.workers,
    )
    if uvicorn_settings.model_extra:
        logger.debug(f"Passing extra Uvicorn options: {uvicorn_settings.model_extra}")
        uvicorn_config_params.update(uvicorn_settings.model_extra)

    def run_uvicorn_server():
        try:
            config = uvicorn.Config(**uvicorn_config_params)
            server = uvicorn.Server(config)
            logger.info(
                f"Uvicorn Server instance created for http://{uvicorn_settings.host}:{uvicorn_settings.port}"
            )
            server.run()
        except Exception as e:
            logger.exception(f"Uvicorn server failed to start: {e}")

    threading.Thread(
        target=run_uvicorn_server, daemon=True, name="uvicorn_thread"
    ).start()
    logger.info(
        f"Admin Web Server started on http://{uvicorn_settings.host}:{uvicorn_settings.port}"
    )
