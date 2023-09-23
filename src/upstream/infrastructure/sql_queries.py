CREATE_FILE_MONITORING_TABLE = """
CREATE TABLE IF NOT EXISTS file_monitoring (
    id INT AUTO_INCREMENT PRIMARY KEY,
    stage VARCHAR(10) NOT NULL,
    src_path VARCHAR(255) NOT NULL,
    timestamp DATETIME NOT NULL
);"""

CREATE_STAGE_EXECUTION_TABLE = """
CREATE TABLE IF NOT EXISTS stage_execution (
    id INT AUTO_INCREMENT PRIMARY KEY,
    stage_name VARCHAR(10) NOT NULL,
    start_time DATETIME NOT NULL,
    end_time DATETIME 
);"""


START_NEW_JOB = """
INSERT INTO stage_execution (stage_name, start_time) VALUES (?, ?)
RETURNING id;
"""

END_JOB = """
UPDATE stage_execution SET end_time = ? WHERE id = ?
"""