CREATE_FILE_MONITORING_TABLE = """
CREATE TABLE IF NOT EXISTS file_monitoring (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    stage VARCHAR(10) NOT NULL,
    src_path VARCHAR(255) NOT NULL,
    timestamp DATETIME NOT NULL
);"""

CREATE_STAGE_EXECUTION_TABLE = """
CREATE TABLE IF NOT EXISTS stage_execution (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    stage_name VARCHAR(10) NOT NULL,
    start_time DATETIME NOT NULL,
    end_time DATETIME 
);"""


INSERT_NEW_FILE = "INSERT INTO file_monitoring (stage, src_path, timestamp) VALUES (?, ?, ?)"


START_NEW_JOB = """
INSERT INTO stage_execution (stage_name, start_time) VALUES (?, ?)
RETURNING id;
"""

END_JOB = """
UPDATE stage_execution SET end_time = ? WHERE id = ?
"""