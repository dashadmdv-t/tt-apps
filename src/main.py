from contextlib import asynccontextmanager

from fastapi import FastAPI, Security

from src.api.router import api_router
from src.api.v1.routers import payments as payments_module
from src.container import Container
from src.dependencies.auth import verify_api_key


def create_app() -> FastAPI:
    container = Container()
    container.wire(modules=[payments_module])

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        broker = container.messaging().broker
        await broker.connect()
        await container.messaging().declare_topology()
        yield
        close = getattr(broker, "close", None)
        if callable(close):
            await close()
        await container.database().dispose()

    app = FastAPI(
        lifespan=lifespan,
        dependencies=[Security(verify_api_key)],
    )
    app.include_router(api_router)
    return app


app = create_app()
