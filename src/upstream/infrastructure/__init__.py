from .database import Database
from .datalake import set_up_local_data_lake
from upstream.common.config import InfraConfig

db = Database(InfraConfig.app_data_dir)  # singleton instance
