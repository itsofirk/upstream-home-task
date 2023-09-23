from celery import Celery
from upstream.common.config import InfraConfig, AppConfig
from upstream.logic import bronze, silver, gold

app_name = AppConfig.app_name
broker = InfraConfig.get_redis_broker()

# Initialize Celery
app = Celery(app_name, broker=broker)

# Define tasks
app.task(bronze)
app.task(silver)
app.task(gold)
