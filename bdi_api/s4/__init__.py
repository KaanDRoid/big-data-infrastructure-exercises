from fastapi import FastAPI

from .exercise import s4  # s4 Router

app = FastAPI()

# Router
app.include_router(s4)
