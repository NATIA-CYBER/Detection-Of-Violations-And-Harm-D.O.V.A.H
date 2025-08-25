from sqlalchemy import text

def _has_label_column(self) -> bool:
    q = text("""
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema='public'
          AND table_name='window_features'
          AND column_name='label'
        LIMIT 1
    """)
    with self.engine.connect() as c:
        return c.execute(q).fetchone() is not None


def get_detection_latencies(self, start_time, end_time):
    q = text("""
        WITH per_session AS (
          SELECT
            w.session_id,
            MIN(w.ts) AS first_event_time,
            MIN(d.created_at) FILTER (WHERE d.score >= :threshold) AS detection_time
          FROM window_features w
          LEFT JOIN detections d ON d.window_id = w.id
          WHERE w.ts BETWEEN :start_time AND :end_time
          GROUP BY w.session_id
        )
        SELECT EXTRACT(EPOCH FROM (detection_time - first_event_time)) * 1000 AS latency_ms
        FROM per_session
        WHERE detection_time IS NOT NULL
    """)
    try:
        with self.engine.connect() as c:
            rows = c.execute(q, {
                "start_time": start_time,
                "end_time": end_time,
                "threshold": self.config.score_threshold
            })
            return [float(r.latency_ms) for r in rows]
    except Exception as e:
        logger.error(f"Error getting detection latencies: {e}")
        return []


def get_confusion_matrix(self, start_time, end_time):
    uses_label = self._has_label_column()
    if not uses_label:
        logger.warning("No 'label' column in window_features; skipping supervised metrics.")
        return (0, 0, 0, 0)

    q = text("""
        WITH scored AS (
          SELECT
            w.session_id,
            MAX(d.score) AS max_score,
            BOOL_OR(w.label = 'malicious') AS is_malicious
          FROM window_features w
          LEFT JOIN detections d ON d.window_id = w.id
          WHERE w.ts BETWEEN :start_time AND :end_time
          GROUP BY w.session_id
        )
        SELECT
          COUNT(*) FILTER (WHERE max_score >= :threshold AND is_malicious) AS tp,
          COUNT(*) FILTER (WHERE max_score >= :threshold AND NOT is_malicious) AS fp,
          COUNT(*) FILTER (WHERE max_score <  :threshold AND NOT is_malicious) AS tn,
          COUNT(*) FILTER (WHERE max_score <  :threshold AND is_malicious)  AS fn
        FROM scored
    """)
    try:
        with self.engine.connect() as c:
            row = c.execute(q, {
                "start_time": start_time,
                "end_time": end_time,
                "threshold": self.config.score_threshold
            }).first()
            return (row.tp or 0, row.fp or 0, row.tn or 0, row.fn or 0)
    except Exception as e:
        logger.error(f"Error getting confusion matrix: {e}")
        return (0, 0, 0, 0)


def get_precision_at_k(self, start_time, end_time):
    uses_label = self._has_label_column()
    if not uses_label:
        logger.warning("No 'label' column; precision@k unavailable.")
        return {k: None for k in self.config.precision_k}

    q = text("""
        WITH per_session AS (
          SELECT
            w.session_id,
            MAX(d.score) AS score,
            BOOL_OR(w.label = 'malicious') AS is_malicious
          FROM window_features w
          LEFT JOIN detections d ON d.window_id = w.id
          WHERE w.ts BETWEEN :start_time AND :end_time
          GROUP BY w.session_id
        ),
        ordered AS (
          SELECT
            session_id, score, is_malicious,
            ROW_NUMBER() OVER (ORDER BY score DESC NULLS LAST) AS rk
          FROM per_session
        )
        SELECT rk, is_malicious FROM ordered ORDER BY rk
    """)
    try:
        with self.engine.connect() as c:
            rows = list(c.execute(q, {"start_time": start_time, "end_time": end_time}))
        dets = [(r.rk, r.is_malicious) for r in rows]
        out = {}
        for k in self.config.precision_k:
            if k > len(dets): out[k] = None; continue
            top = dets[:k]
            tp = sum(1 for _, m in top if m)
            out[k] = tp / k
        return out
    except Exception as e:
        logger.error(f"Error calculating precision@k: {e}")
        return {k: None for k in self.config.precision_k}
