"""SHAP-based model explanations for window features."""
import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import shap
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ShapExplainer:
    def __init__(self, db_url: str, model_path: str):
        self.engine = create_engine(db_url)
        self.model = self._load_model(model_path)
        self.explainer = shap.KernelExplainer(
            self.model.predict_proba,
            shap.sample(self._get_background_data(), 100)
        )
        
    def _load_model(self, model_path: str):
        """Load the trained model from disk."""
        try:
            import joblib
            return joblib.load(model_path)
        except Exception as e:
            logger.error(f"Failed to load model from {model_path}: {e}")
            raise ValueError(f"Model not found or invalid: {e}")
        
    def _get_background_data(self) -> np.ndarray:
        """Get background dataset for SHAP."""
        query = text("""
            SELECT 
                w.event_count,
                w.unique_components,
                w.error_ratio,
                w.template_entropy,
                w.component_entropy
            FROM window_features w
            ORDER BY RANDOM()
            LIMIT 1000
        """)
        
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)
            return df.values
            
    def explain_detection(self, detection_id: str) -> Optional[Dict]:
        """Generate SHAP explanation for a detection."""
        query = text("""
            SELECT 
                w.event_count,
                w.unique_components,
                w.error_ratio,
                w.template_entropy,
                w.component_entropy
            FROM detections d
            JOIN window_features w ON d.window_id = w.id
            WHERE d.id = :detection_id
        """)
        
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn, params={"detection_id": detection_id})
                if df.empty:
                    logger.warning(f"No data found for detection {detection_id}")
                    return None
                    
                # Generate SHAP values
                shap_values = self.explainer.shap_values(df.values)[1]  # Class 1 for anomaly
                
                # Get feature names and values
                feature_names = [
                    "event_count",
                    "unique_components",
                    "error_ratio", 
                    "template_entropy",
                    "component_entropy"
                ]
                
                # Ensure all required features are present
                if not all(name in df.columns for name in feature_names):
                    missing = [name for name in feature_names if name not in df.columns]
                    logger.error(f"Missing required features: {missing}")
                    return None
                
                # Create explanation dictionary
                explanation = {
                    "feature_importance": dict(zip(
                        feature_names,
                        np.abs(shap_values[0]).tolist()
                    )),
                    "feature_effects": dict(zip(
                        feature_names,
                        shap_values[0].tolist()
                    )),
                    "base_score": float(self.explainer.expected_value[1])
                }
                
                return explanation
                
        except Exception as e:
            logger.error(f"Error explaining detection {detection_id}: {e}")
            return None
            
    def save_explanation(self, detection_id: str) -> bool:
        """Save SHAP explanation to database."""
        explanation = self.explain_detection(detection_id)
        if not explanation:
            return False
            
        query = text("""
            INSERT INTO shap_explanations (
                detection_id,
                feature_importance,
                feature_effects,
                base_score
            ) VALUES (
                :detection_id,
                :feature_importance,
                :feature_effects,
                :base_score
            )
            ON CONFLICT (detection_id) DO UPDATE SET
                feature_importance = EXCLUDED.feature_importance,
                feature_effects = EXCLUDED.feature_effects,
                base_score = EXCLUDED.base_score
        """)
        
        try:
            with self.engine.connect() as conn:
                conn.execute(
                    query,
                    {
                        "detection_id": detection_id,
                        "feature_importance": explanation["feature_importance"],
                        "feature_effects": explanation["feature_effects"],
                        "base_score": explanation["base_score"]
                    }
                )
                return True
        except Exception as e:
            logger.error(f"Error saving explanation for {detection_id}: {e}")
            return False
