"""
Isolation Forest anomaly detection for windowed HDFS features.
"""

from __future__ import annotations

import logging
import os
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger(__name__)


class IForestConfig(BaseModel):
    """Isolation Forest configuration."""
    feature_columns: List[str] = Field(
        default=[
            "event_count",
            "unique_components",
            "error_ratio",
            "template_entropy",
            "component_entropy",
        ],
        description="Features to use for anomaly detection",
    )
    n_estimators: int = Field(default=100, description="Number of isolation trees")
    contamination: float = Field(default=0.10, description="Expected proportion of anomalies")


class IForestModel:
    """Isolation Forest anomaly detector with StandardScaler."""

    def __init__(self, config: Optional[IForestConfig] = None):
        self.config = config or IForestConfig()
        seed = int(os.getenv("DOVAH_ANALYSIS_SEED", "42"))
        self.model = IsolationForest(
            n_estimators=self.config.n_estimators,
            contamination=self.config.contamination,
            random_state=seed,
        )
        np.random.seed(seed)
        self.scaler = StandardScaler()
        self._fitted = False

    def _extract_features(self, events: List[Dict]) -> Tuple[pd.DataFrame, List[Dict]]:
        """Extract features from events with NaN/inf handling."""
        feats: List[Dict[str, float]] = []
        keep: List[Dict] = []
        cols = self.config.feature_columns

        for ev in events:
            ok = True
            f: Dict[str, float] = {}
            for c in cols:
                if c not in ev:
                    ok = False
                    break
                val = ev[c]
                # Reject NaN/inf/non-numeric
                try:
                    val_f = float(val)
                except Exception:
                    ok = False
                    break
                if not np.isfinite(val_f):
                    ok = False
                    break
                f[c] = val_f
            if ok:
                feats.append(f)
                keep.append(ev)
            else:
                logger.debug("Skipping invalid event: %s", ev)

        if not feats:
            return pd.DataFrame(columns=cols), []
        return pd.DataFrame(feats), keep

    def fit(self, events: List[Dict]) -> None:
        """Fit scaler and model."""
        X_df, _ = self._extract_features(events)
        if X_df.empty:
            raise ValueError("No valid features to train on")
        X = self.scaler.fit_transform(X_df.values)
        self.model.fit(X)
        self._fitted = True
        logger.info("IsolationForest trained on %d samples (%d features)", X.shape[0], X.shape[1])

    def predict(self, events: List[Dict]) -> Dict[str, Dict[str, float]]:
        """
        Predict anomaly scores for events, aggregated per session_id (latest ts kept).

        Returns:
            { session_id: { "score": float(0..1), "ts": timestamp_like } }
        """
        if not self._fitted:
            raise RuntimeError("IForestModel.predict called before fit/load")

        X_df, valid = self._extract_features(events)
        if X_df.empty:
            logger.warning("No valid features to score")
            return {}

        X = self.scaler.transform(X_df.values)
        raw = self.model.score_samples(X)  # higher => more normal (by IF convention)

        # Map to anomaly-like score in [0,1]; higher => more anomalous
        if len(raw) > 1:
            rmin, rmax = float(np.min(raw)), float(np.max(raw))
            if rmax == rmin:
                s = np.full_like(raw, 0.5, dtype=float)
            else:
                s = 1.0 - (raw - rmin) / (rmax - rmin)
        else:
            s = 1.0 / (1.0 + np.exp(-abs(raw)))  # single-row fallback

        out: Dict[str, Dict[str, float]] = {}
        for ev, sc in zip(valid, s):
            sid = ev.get("session_id")
            ts = ev.get("ts")
            if sid is None or ts is None:
                logger.debug("Skipping event without session_id/ts: %s", ev)
                continue
            # keep latest ts per session_id
            if sid not in out or (ts is not None and ts > out[sid]["ts"]):
                out[sid] = {"score": float(np.clip(sc, 0.0, 1.0)), "ts": ts}
        return out

    def save(self, path: str) -> None:
        import joblib
        joblib.dump(
            {"model": self.model, "scaler": self.scaler, "config": self.config, "_fitted": True},
            path,
        )

    @classmethod
    def load(cls, path: str) -> "IForestModel":
        import joblib
        data = joblib.load(path)
        m = cls(config=data["config"])
        m.model = data["model"]
        m.scaler = data["scaler"]
        m._fitted = bool(data.get("_fitted", True))
        return m
