import os
import shutil
from datetime import datetime

# Paths
DB_PATH = "/home/ubuntu/nse/prices.db"
BACKUP_DIR = "/home/ubuntu/nse/backups"
MAX_BACKUPS = 5

# Ensure backup folder exists
os.makedirs(BACKUP_DIR, exist_ok=True)

# Create timestamped backup name
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
backup_file = os.path.join(BACKUP_DIR, f"prices_backup_{timestamp}.db")

# Copy database file
shutil.copy2(DB_PATH, backup_file)
print(f"📦 Backup created: {backup_file}")

# ---- Retention Policy (Keep Only Last 5) ----
backups = sorted(
    [f for f in os.listdir(BACKUP_DIR) if f.startswith("prices_backup_")],
    key=lambda x: os.path.getctime(os.path.join(BACKUP_DIR, x))
)

if len(backups) > MAX_BACKUPS:
    old_files = backups[:-MAX_BACKUPS]  # everything except newest 5
    for old_file in old_files:
        old_path = os.path.join(BACKUP_DIR, old_file)
        os.remove(old_path)
        print(f"🗑️ Removed old backup: {old_file}")
