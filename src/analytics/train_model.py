"""
Training Script for FPL Predictor Model.

Run this script to train and save the best XGBoost model.
Experiments with different feature sets and selects the best non-overfitting one.
"""
import asyncio
import structlog
from src.analytics.predictor import FPLPredictor
from src.database.db_manager import DBManager

# Configure logging
structlog.configure(
    processors=[
        structlog.processors.JSONRenderer()
    ]
)
logger = structlog.get_logger()


async def train_model():
    """Train and save the best prediction model."""
    db = DBManager()
    await db.connect()
    
    try:
        predictor = FPLPredictor(db)
        
        logger.info("starting_feature_experimentation")
        
        # Train the best model based on experimentation
        metrics = await predictor.train_best_model()
        
        if "error" not in metrics:
            predictor.save_model()
            logger.info("training_complete", **metrics)
        else:
            logger.error("training_failed", **metrics)
    
    finally:
        await db.disconnect()


if __name__ == "__main__":
    asyncio.run(train_model())
