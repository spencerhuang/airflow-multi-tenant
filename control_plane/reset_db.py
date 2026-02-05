import asyncio
import sys
import os

# Add the project root to sys.path (parent of control_plane directory)
sys.path.append(os.path.dirname(os.getcwd()))

from control_plane.app.core.database import engine, Base
# Import all models to ensure metadata is populated
from control_plane.app.models import (
    customer,
    workspace,
    auth,
    workflow,
    access_point,
    integration,
    integration_run
)

async def reset():
    print("Dropping all tables...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    print("All tables dropped.")
    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(reset())
