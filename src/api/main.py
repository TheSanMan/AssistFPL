"""
FastAPI Backend for AssistFPL.

Provides REST API endpoints for player predictions, insights, and chat.
"""
import os
from contextlib import asynccontextmanager
from typing import List, Optional

import structlog
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from src.database.db_manager import DBManager
from src.analytics.predictor import FPLPredictor
from src.analytics.insights import InsightGenerator
from src.agent.agent import FPLAgent

# Configure logging
structlog.configure(
    processors=[structlog.processors.JSONRenderer()]
)
logger = structlog.get_logger()

# Global instances (initialized on startup)
db: DBManager = None
predictor: FPLPredictor = None
insight_generator: InsightGenerator = None
agent: FPLAgent = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup and shutdown lifecycle."""
    global db, predictor, insight_generator, agent
    
    # Startup
    db = DBManager()
    await db.connect()
    predictor = FPLPredictor(db)
    predictor.load_model()
    insight_generator = InsightGenerator(db)
    agent = FPLAgent(db, predictor, insight_generator)
    logger.info("api_startup_complete")
    
    yield
    
    # Shutdown
    await db.disconnect()
    logger.info("api_shutdown_complete")


app = FastAPI(
    title="AssistFPL API",
    description="AI-powered Fantasy Premier League assistant",
    version="1.0.0",
    lifespan=lifespan
)

# CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ==================== MODELS ====================

class PlayerPrediction(BaseModel):
    player_id: int
    web_name: str
    team_id: int
    position: int
    price: float
    predicted_points: float
    api_ep_next: float
    form_points_3: float


class Insight(BaseModel):
    category: str
    insight: str
    sentiment: str


class TransferRequest(BaseModel):
    budget: float
    position: Optional[int] = None
    exclude_players: Optional[List[int]] = None
    top_n: int = 5


class ChatRequest(BaseModel):
    message: str


class ChatResponse(BaseModel):
    response: str


# ==================== ENDPOINTS ====================

@app.get("/")
async def root():
    """Health check."""
    return {"status": "ok", "service": "AssistFPL API"}


@app.get("/players", response_model=List[dict])
async def get_players(limit: int = Query(50, le=1000)):
    """Get all players with basic info."""
    players = await db.get_all_players()
    return players[:limit]


@app.get("/players/{player_id}")
async def get_player(player_id: int):
    """Get a single player by ID."""
    player = await db.get_player(player_id)
    if not player:
        raise HTTPException(status_code=404, detail="Player not found")
    return player


@app.get("/players/{player_id}/prediction")
async def get_player_prediction(player_id: int):
    """Get prediction for a single player."""
    prediction = await predictor.predict_player(player_id)
    if prediction is None:
        raise HTTPException(status_code=404, detail="Could not generate prediction")
    return {"player_id": player_id, "predicted_points": prediction}


@app.get("/players/{player_id}/insights", response_model=List[Insight])
async def get_player_insights(player_id: int, opponent_id: Optional[int] = None):
    """Get insights for a player."""
    insights = await insight_generator.get_all_insights(player_id, opponent_id)
    return insights


@app.get("/players/{player_id}/history")
async def get_player_history(player_id: int):
    """Get gameweek history for a player."""
    history = await db.get_player_history(player_id)
    return history


@app.get("/players/{player_id}/past-seasons")
async def get_player_past_seasons(player_id: int):
    """Get past season summaries for a player."""
    seasons = await db.get_player_past_seasons(player_id)
    return seasons


@app.get("/players/{player_id}/fixtures")
async def get_player_fixtures(player_id: int):
    """Get upcoming fixtures for a player."""
    player = await db.get_player(player_id)
    if not player:
        raise HTTPException(status_code=404, detail="Player not found")
    
    fixtures = await db.get_upcoming_fixtures(player['team_id'])
    return fixtures


@app.get("/predictions/top", response_model=List[PlayerPrediction])
async def get_top_predictions(
    limit: int = Query(20, le=100),
    position: Optional[int] = None
):
    """Get top predicted players."""
    all_predictions = await predictor.predict_all_players()
    
    if position:
        all_predictions = [p for p in all_predictions if p['position'] == position]
    
    return all_predictions[:limit]


@app.post("/transfers/suggest", response_model=List[PlayerPrediction])
async def suggest_transfers(request: TransferRequest):
    """Get transfer suggestions based on budget and constraints."""
    suggestions = await predictor.get_transfer_suggestions(
        budget=request.budget,
        position=request.position,
        exclude_players=request.exclude_players,
        top_n=request.top_n
    )
    return suggestions


@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """
    Chat with the FPL assistant.
    Uses LLM if GOOGLE_API_KEY is set, otherwise falls back to intent detection.
    """
    response = await agent.chat(request.message)
    return ChatResponse(response=response)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.api.main:app", host="0.0.0.0", port=8000, reload=True)
