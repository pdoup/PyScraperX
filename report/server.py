import logging
import pathlib
import threading
from typing import Any, Dict

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from config import UvicornServerSettings, settings
from report.job_models import (RestartJobBatchResponse, RestartJobRequest,
                               RestartJobResponse)
from report.state_manager import (get_all_job_statuses,
                                  restart_all_failed_jobs, restart_job)

logger = logging.getLogger("WebScraper")

app = FastAPI(title="Web Scraper Admin Panel")
app.mount(
    "/static",
    StaticFiles(directory=pathlib.Path(__file__).parent / "static"),
    name="static",
)
templates = Jinja2Templates(directory=pathlib.Path(__file__).parent / "index")


@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """Serve the admin dashboard HTML from file."""
    try:
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "update_interval": settings.update_interval_ms,
            },
        )
    except Exception as e:
        return HTMLResponse(
            content=f"<h1>Error loading dashboard</h1><p>{str(e)}</p>", status_code=500
        )


@app.get("/api/jobs")
async def get_jobs_status():
    """API endpoint to get the current status of all jobs."""
    return get_all_job_statuses()


@app.post("/api/jobs/restart")
async def restart_failed_job(payload: RestartJobRequest):
    """
    Restarts a job that is in a 'Failed' or 'Permanently Failed' state.

    This endpoint validates the job's current state before resetting its
    retry count and error message, allowing the scheduler to attempt the
    job again.
    """
    restart_response: RestartJobResponse = await restart_job(job_id=payload.jobId)
    if not restart_response.success:
        logger.warning(f"Failed to restart job {payload.jobId}, job remains [failed].")
        if "not found" in restart_response.message:
            raise HTTPException(status_code=404, detail=restart_response.message)
        else:
            raise HTTPException(status_code=409, detail=restart_response.message)

    logger.info(f"Job {payload.jobId} restarted.")
    return JSONResponse(
        {"status": restart_response.success, "message": restart_response.message}
    )


@app.post("/api/jobs/restart_all", response_model=RestartJobBatchResponse)
async def restart_all_permanently_failed_jobs():
    """
    Restarts all jobs currently in the 'Permanently Failed' state.

    This endpoint is designed for the 'Restart All' button that appears
    when filtering for permanently failed jobs. It runs the restart
    operations concurrently for efficiency.
    """
    try:
        result = await restart_all_failed_jobs()
        logger.info(
            f"{result.restarted_count}/{result.restarted_count+result.failed_count} jobs scheduled for restart."
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
