from pyspark.sql import SparkSession
from pyspark.sql.streaming import DataStreamReader
from loguru import logger


def configure_autoloader(
    spark: SparkSession,
    source_path: str,
    source_format: str,
    schema_location: str,
    extra_options: dict | None = None,
) -> DataStreamReader:
    """Return a configured Auto Loader DataStreamReader for the given source.

    Supports JSON, CSV, and Parquet.  Schema evolution is enabled via
    cloudFiles.schemaEvolutionMode=addNewColumns so newly introduced fields
    are captured without manual intervention.

    Args:
        spark: Active SparkSession.
        source_path: Cloud storage path to the landing zone (ADLS or S3).
        source_format: One of "json", "csv", or "parquet".
        schema_location: Cloud path where Auto Loader persists the inferred schema.
        extra_options: Optional dict of additional cloudFiles / format options.

    Returns:
        A DataStreamReader ready for .load().
    """
    supported_formats = {"json", "csv", "parquet", "avro", "text"}
    if source_format not in supported_formats:
        raise ValueError(f"Unsupported source format: {source_format}. Choose from {supported_formats}")

    logger.info(f"Configuring Auto Loader | format={source_format} path={source_path}")

    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", source_format)
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.backfillInterval", "1 day")
        .option("cloudFiles.useStrictGlobber", "false")
        .option("pathGlobFilter", _default_glob(source_format))
    )

    if source_format == "csv":
        reader = (
            reader
            .option("header", "true")
            .option("inferSchema", "true")
            .option("multiLine", "true")
            .option("escape", '"')
        )
    elif source_format == "json":
        reader = reader.option("multiLine", "false")

    if extra_options:
        for k, v in extra_options.items():
            reader = reader.option(k, v)

    return reader.load(source_path)


def _default_glob(source_format: str) -> str:
    globs = {
        "json": "*.json",
        "csv": "*.csv",
        "parquet": "*.parquet",
        "avro": "*.avro",
        "text": "*.txt",
    }
    return globs.get(source_format, "*")
