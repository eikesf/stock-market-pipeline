import sys
from pathlib import Path
from loguru import logger

# Make sure the logs folder exists dynamically
BASE_DIR = Path(__file__).resolve().parent.parent.parent
log_dir = BASE_DIR / "data" / "logs"
log_dir.mkdir(parents=True, exist_ok=True)

# Remove loguru default settings
logger.remove(0)

# Terminal log settings (stdout)
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level="INFO"
)

# Log file settings (DEBUG level)
logger.add(
    str(log_dir / "pipeline.log"),
    rotation="10 MB",
    retention="10 days",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
    level="DEBUG",
    encoding="utf-8"
)