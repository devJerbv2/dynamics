from uvicorn import run
from database.category import get_category
from database.client import update_client_info
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, HTTPException, status

app = FastAPI(
    title="Dynamics",
    description="Client Preferences",
    docs_url="/",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins="*",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=[
        "Authorization",
        "Content-Type",
        "Accept",
    ],
)


@app.post("/create_preference")
async def create_preference(category: str, client_name: str) -> dict:
    # is_updated = update_client_info(client_name=client_name, category=category)

    # if not is_updated:
    #     raise HTTPException(
    #         status_code=status.HTTP_400_BAD_REQUEST,
    #         detail="Unable to update client info",
    #     )

    category = get_category(category)

    if category is None:
        return "No existing category found"

    return category


if __name__ == "__main__":
    run("main:app", host="0.0.0.0", port=8000)
