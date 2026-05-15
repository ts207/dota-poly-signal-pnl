import os
import time
import csv
from storage import CsvLogger

def test_csv_logger_flush(tmp_path):
    log_file = tmp_path / "test_flush.csv"
    headers = ["ts", "val"]
    
    logger = CsvLogger(str(log_file), headers)
    
    # Append many rows quickly
    for i in range(100):
        logger.append({"ts": time.time(), "val": i})
    
    # Stop the logger, which should wait for the worker thread to finish
    logger.stop()
    
    # Verify all rows are written
    assert log_file.exists()
    with open(log_file, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        assert len(rows) == 100
        for i, row in enumerate(rows):
            assert int(row["val"]) == i
