"""
XGBoost Predictor for FPL Points.

Trains a model to predict a player's points for the next gameweek.
Includes feature experimentation to find the best non-overfitting model.
"""
import pickle
import os
import structlog
from typing import List, Optional, Tuple, Dict
from dataclasses import asdict
import numpy as np

from src.analytics.features import FeatureEngineer, PlayerFeatures
from src.database.db_manager import DBManager

logger = structlog.get_logger()

# Different feature sets to experiment with
FEATURE_SETS = {
    "api_only": [
        'position', 'price', 'api_form', 'api_ict_index', 
        'api_selected_pct', 'api_ppg'
    ],
    "calculated_only": [
        'position', 'price', 'form_points_3', 'form_points_5',
        'form_xg_3', 'form_xa_3', 'form_minutes_3',
        'total_points_season', 'games_played'
    ],
    "advanced": [
        'position', 'price', 
        'form_points_3', 'form_xg_3', 'form_xa_3', 
        'form_creativity_3', 'form_threat_3', 'form_influence_3',
        'next_fixture_difficulty', 'chance_of_playing',
        'total_points_season', 'games_played'
    ],
    "combined": [
        'position', 'price', 'api_form', 'api_ict_index',
        'form_points_3', 'form_points_5', 'form_xg_3', 'form_xa_3',
        'form_creativity_3', 'form_threat_3',
        'next_fixture_difficulty', 'chance_of_playing',
        'total_points_season', 'games_played'
    ],
    "minimal": [
        'position', 'price', 'api_form', 'form_points_3', 'total_points_season'
    ]
}


class FPLPredictor:
    """XGBoost-based predictor for FPL player points."""
    
    MODEL_PATH = "models/xgb_predictor.pkl"
    
    def __init__(self, db: DBManager):
        """
        Initialize with shared DBManager.
        
        Args:
            db: Shared database manager
        """
        self.db = db
        self.model = None
        self.feature_engineer = FeatureEngineer(db)
        self.feature_cols: List[str] = []

    def _features_to_array(self, features: PlayerFeatures, feature_cols: List[str]) -> np.ndarray:
        """Convert PlayerFeatures to numpy array for prediction."""
        data = asdict(features)
        return np.array([[data[col] for col in feature_cols]])

    async def prepare_training_data(self, feature_cols: List[str]) -> Tuple[np.ndarray, np.ndarray, List[int]]:
        """
        Prepare training data from the database.
        
        Returns:
            X: Feature matrix
            y: Target values (using api_ep_next as proxy for actual next GW points)
            player_ids: List of player IDs for reference
        """
        all_features = await self.feature_engineer.build_all_player_features()
        
        X = []
        y = []
        player_ids = []
        
        for pf in all_features:
            if pf.games_played >= 3:  # Need enough history
                try:
                    row = [getattr(pf, col) for col in feature_cols]
                    X.append(row)
                    # Use api_ep_next as target (FPL's official prediction)
                    y.append(pf.api_ep_next)
                    player_ids.append(pf.player_id)
                except AttributeError as e:
                    logger.warning("missing_feature", player_id=pf.player_id, error=str(e))
        
        return np.array(X), np.array(y), player_ids

    def train(self, X: np.ndarray, y: np.ndarray, feature_set_name: str = "combined") -> dict:
        """
        Train the XGBoost model with cross-validation.
        
        Args:
            X: Feature matrix
            y: Target values
            feature_set_name: Name of the feature set for logging
            
        Returns:
            Dictionary with training metrics
        """
        try:
            from xgboost import XGBRegressor
            from sklearn.model_selection import cross_val_score, train_test_split
            from sklearn.metrics import mean_absolute_error, mean_squared_error
        except ImportError:
            logger.error("missing_dependencies", msg="Install xgboost and scikit-learn")
            return {"error": "Missing dependencies"}
        
        # Use cross-validation to detect overfitting
        model = XGBRegressor(
            n_estimators=100,
            max_depth=4,  # Reduced to prevent overfitting
            learning_rate=0.1,
            min_child_weight=3,  # Regularization
            subsample=0.8,  # Prevents overfitting
            random_state=42
        )
        
        # 5-fold cross-validation
        cv_scores = cross_val_score(model, X, y, cv=5, scoring='neg_mean_absolute_error')
        cv_mae = -cv_scores.mean()
        cv_std = cv_scores.std()
        
        # Final train/test split for reporting
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        model.fit(X_train, y_train)
        self.model = model
        
        # Evaluate
        y_pred = model.predict(X_test)
        test_mae = mean_absolute_error(y_test, y_pred)
        test_rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        
        # Detect overfitting: if test MAE is much worse than CV MAE
        overfitting_risk = test_mae > cv_mae * 1.2
        
        logger.info("model_trained", 
                    feature_set=feature_set_name,
                    cv_mae=cv_mae, 
                    cv_std=cv_std,
                    test_mae=test_mae, 
                    test_rmse=test_rmse,
                    overfitting_risk=overfitting_risk)
        
        return {
            "feature_set": feature_set_name,
            "cv_mae": float(cv_mae),
            "cv_std": float(cv_std),
            "test_mae": float(test_mae),
            "test_rmse": float(test_rmse),
            "train_samples": len(X_train),
            "test_samples": len(X_test),
            "overfitting_risk": overfitting_risk
        }

    async def experiment_feature_sets(self) -> Dict[str, dict]:
        """
        Experiment with all feature sets and return metrics.
        """
        results = {}
        
        for name, feature_cols in FEATURE_SETS.items():
            logger.info("experimenting_feature_set", name=name)
            X, y, _ = await self.prepare_training_data(feature_cols)
            
            if len(X) < 100:
                logger.warning("insufficient_data", feature_set=name, count=len(X))
                continue
            
            metrics = self.train(X, y, name)
            results[name] = metrics
        
        # Find best non-overfitting model
        best_set = None
        best_mae = float('inf')
        
        for name, metrics in results.items():
            if not metrics.get('overfitting_risk', True) and metrics.get('cv_mae', float('inf')) < best_mae:
                best_mae = metrics['cv_mae']
                best_set = name
        
        if best_set:
            logger.info("best_feature_set", name=best_set, cv_mae=best_mae)
            results['_best'] = best_set
        
        return results

    async def train_best_model(self) -> dict:
        """
        Train the best performing model based on experimentation.
        """
        results = await self.experiment_feature_sets()
        
        best_set = results.get('_best', 'combined')
        self.feature_cols = FEATURE_SETS[best_set]
        
        X, y, _ = await self.prepare_training_data(self.feature_cols)
        metrics = self.train(X, y, best_set)
        
        return metrics

    def save_model(self):
        """Save the trained model and feature columns to disk."""
        os.makedirs(os.path.dirname(self.MODEL_PATH), exist_ok=True)
        with open(self.MODEL_PATH, 'wb') as f:
            pickle.dump({
                'model': self.model,
                'feature_cols': self.feature_cols
            }, f)
        logger.info("model_saved", path=self.MODEL_PATH)

    def load_model(self) -> bool:
        """Load a previously trained model."""
        if os.path.exists(self.MODEL_PATH):
            with open(self.MODEL_PATH, 'rb') as f:
                data = pickle.load(f)
                if isinstance(data, dict):
                    self.model = data['model']
                    self.feature_cols = data['feature_cols']
                else:
                    # Legacy format
                    self.model = data
                    self.feature_cols = FEATURE_SETS['combined']
            logger.info("model_loaded", path=self.MODEL_PATH)
            return True
        return False

    async def predict_player(self, player_id: int) -> Optional[float]:
        """Predict points for a single player."""
        if self.model is None:
            if not self.load_model():
                logger.error("no_model_available")
                return None
        
        features = await self.feature_engineer.build_player_features(player_id)
        if features is None:
            return None
        
        X = self._features_to_array(features, self.feature_cols)
        prediction = self.model.predict(X)[0]
        
        return float(prediction)

    async def predict_all_players(self) -> List[dict]:
        """Predict points for all players."""
        if self.model is None:
            if not self.load_model():
                return []
        
        all_features = await self.feature_engineer.build_all_player_features()
        predictions = []
        
        for pf in all_features:
            X = self._features_to_array(pf, self.feature_cols)
            pred = self.model.predict(X)[0]
            predictions.append({
                "player_id": pf.player_id,
                "web_name": pf.web_name,
                "team_id": pf.team_id,
                "position": pf.position,
                "price": pf.price,
                "predicted_points": round(float(pred), 2),
                "api_ep_next": pf.api_ep_next,  # For comparison
                "form_points_3": pf.form_points_3
            })
        
        predictions.sort(key=lambda x: x['predicted_points'], reverse=True)
        return predictions

    async def get_transfer_suggestions(
        self, 
        budget: float, 
        position: Optional[int] = None,
        exclude_players: Optional[List[int]] = None,
        top_n: int = 5
    ) -> List[dict]:
        """Get transfer suggestions based on predictions and budget."""
        all_predictions = await self.predict_all_players()
        exclude_set = set(exclude_players or [])
        
        suggestions = [
            p for p in all_predictions
            if p['price'] <= budget
            and p['player_id'] not in exclude_set
            and (position is None or p['position'] == position)
        ]
        
        return suggestions[:top_n]
