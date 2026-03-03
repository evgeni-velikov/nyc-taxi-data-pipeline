import os


SPARK_IMAGE = os.getenv("SPARK_IMAGE", "nyc-taxi-data-pipeline-spark-master")
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
HOST_PROJECT_PATH= os.getenv("HOST_PROJECT_PATH", "/Users/velikov/Projects/nyc-taxi-data-pipeline")

COMMON_DOCKER_ARGS = {
    "docker_url": "unix://var/run/docker.sock",
    "network_mode": os.getenv("DOCKER_NETWORK", "nyc-taxi-data-pipeline"),
    "mount_tmp_dir": False,
    "auto_remove": True,
    "force_pull": False,
    "tty": False,
    "environment": {
        "PYTHONUNBUFFERED": "1",
        "DBT_USER": "evgeni",
    },
}
