"""Abstract review sources for static replay and streaming."""

from __future__ import annotations

from pyspark import RDD
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.ingest.ingestion_service import IngestionService
from src.streaming.rdd_builder import RDDBuilder


class StaticReviewSource:
    """Loads the static Yelp dataset and returns an enriched RDD."""

    def __init__(self, spark: SparkSession, ingestion: IngestionService) -> None:
        self.spark = spark
        self.ingestion = ingestion
        self.builder = RDDBuilder(spark, ingestion)

    def load(self) -> RDD:
        return self.builder.build_enriched_reviews()


class FileStreamReviewSource:
    """Structured Streaming source that tails newline-delimited review JSON files."""

    def __init__(
        self,
        spark: SparkSession,
        ingestion: IngestionService,
        stream_path: str,
        checkpoint_path: str,
        trigger_seconds: int = 5,
    ) -> None:
        self.spark = spark
        self.ingestion = ingestion
        self.stream_path = stream_path
        self.checkpoint_path = checkpoint_path
        self.trigger_seconds = trigger_seconds
        self.builder = RDDBuilder(spark, ingestion)

    def start(self, aggregator, persistence):
        """Begin reading the stream and processing micro-batches via aggregator."""

        business_df = self.ingestion.load_businesses().dropna(subset=("latitude", "longitude")).cache()
        review_schema = self.ingestion.review_schema()
        stream = (
            self.spark.readStream.schema(review_schema)
            .json(self.stream_path)
            .dropna(subset=("review_id", "business_id", "date"))
        )

        def _process_batch(batch_df: DataFrame, batch_id: int):
            if batch_df.rdd.isEmpty():
                return
            enriched = self.builder.build_enriched_from_frames(business_df, batch_df)
            tables = aggregator.run(enriched)
            persistence.write_stream_batch(tables)

        query = (
            stream.writeStream.foreachBatch(_process_batch)
            .option("checkpointLocation", self.checkpoint_path)
            .trigger(processingTime=f"{self.trigger_seconds} seconds")
            .start()
        )
        return query


class KafkaReviewSource:
    """Structured Streaming source consuming review JSON from Kafka."""

    def __init__(
        self,
        spark: SparkSession,
        ingestion: IngestionService,
        bootstrap_servers: str,
        topic: str,
        checkpoint_path: str,
        starting_offsets: str = "latest",
        trigger_seconds: int = 5,
    ) -> None:
        self.spark = spark
        self.ingestion = ingestion
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.checkpoint_path = checkpoint_path
        self.starting_offsets = starting_offsets
        self.trigger_seconds = trigger_seconds
        self.builder = RDDBuilder(spark, ingestion)

    def start(self, aggregator, persistence):
        business_df = self.ingestion.load_businesses().dropna(subset=("latitude", "longitude")).cache()
        review_schema = self.ingestion.review_schema()
        stream = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", self.topic)
            .option("startingOffsets", self.starting_offsets)
            .load()
        )
        parsed = (
            stream.select(
                F.from_json(F.col("value").cast("string"), review_schema).alias("json")
            )
            .select("json.*")
            .dropna(subset=("review_id", "business_id", "date"))
        )

        def _process_batch(batch_df: DataFrame, batch_id: int):
            if batch_df.rdd.isEmpty():
                return
            enriched = self.builder.build_enriched_from_frames(business_df, batch_df)
            tables = aggregator.run(enriched)
            persistence.write_stream_batch(tables)

        query = (
            parsed.writeStream.foreachBatch(_process_batch)
            .option("checkpointLocation", self.checkpoint_path)
            .trigger(processingTime=f"{self.trigger_seconds} seconds")
            .start()
        )
        return query