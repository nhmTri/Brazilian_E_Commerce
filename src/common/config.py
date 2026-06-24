
# ── Unity Catalog ─────────────────────────────────────────────
CATALOG  = "olist_ecommerce"
BRONZE   = f"{CATALOG}.bronze"
SILVER   = f"{CATALOG}.silver"
GOLD     = f"{CATALOG}.gold"
BUSINESS = f"{CATALOG}.business"

# ── Storage paths ─────────────────────────────────────────────
VOLUME_PATH  = "/Volumes/olist_ecommerce/bronze/raw_files"
CHECKPOINT   = f"{VOLUME_PATH}/checkpoints"

# ── Streaming config ──────────────────────────────────────────
WATERMARK_HOURS       = 24
MAX_FILES_PER_TRIGGER = 10

# ── DQ thresholds ─────────────────────────────────────────────
MAX_DROP_RATE_PCT   = 5.0   # Alert nếu drop > 5%
MAX_DIM_NULL_RATE   = 0.05  # Alert nếu dim null > 5%

# ── Business rules ────────────────────────────────────────────
REVIEW_SCORE_MIN = 1
REVIEW_SCORE_MAX = 5
MAX_INSTALLMENTS  = 24
MAX_LEAD_TIME     = 365

# ── Alert thresholds ──────────────────────────────────────────
ZSCORE_SPIKE_THRESHOLD = 2.0
ZSCORE_DROP_THRESHOLD  = -2.0