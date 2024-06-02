import json
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from threading import Thread
from pyspark import RDD as SparkRDD
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    collect_list,
    explode,
    from_unixtime,
    monotonically_increasing_id,
)
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window
from pyspark.streaming import StreamingContext
from src.sockets import StreamingSender, SocketUser


class Pipeline(metaclass=ABCMeta):
    @abstractmethod
    def transform(self, df: SparkDataFrame) -> SparkDataFrame:
        raise NotImplementedError


class YahooFinancePipeline(Pipeline):
    def __init__(self, target_column: str, smoothing_win_size: int) -> None:
        super().__init__()
        self.target_column = target_column
        self.smoothing_win_size = smoothing_win_size

    def _preprocess(self, df: SparkDataFrame) -> SparkDataFrame:
        df_timestamp = (
            df.select("chart.result.timestamp")
            .withColumn("timestamp", explode("timestamp"))
            .withColumn("timestamp", explode("timestamp"))
        )
        df_price = (
            df.select("chart.result.indicators.quote")
            .withColumn("quote", explode("quote"))
            .select(f"quote.{self.target_column}")
            .withColumnRenamed(self.target_column, "price")
            .withColumn("price", explode("price"))
            .withColumn("price", explode("price"))
        )

        df = (
            df_timestamp.withColumn("id", monotonically_increasing_id())
            .join(df_price.withColumn("id", monotonically_increasing_id()), ["id"])
            .drop("id")
        )

        return df

    def _calculate_MA(self, df: SparkDataFrame) -> SparkDataFrame:
        w = Window().orderBy("timestamp").rowsBetween(-self.smoothing_win_size, 0)
        df = df.withColumn("price_MA", avg("price").over(w))
        return df

    def _format(self, df: SparkDataFrame) -> SparkDataFrame:
        df = df.withColumn(
            "timestamp", from_unixtime("timestamp").cast(TimestampType()).cast("string")
        )
        return df

    def transform(self, df: SparkDataFrame) -> SparkDataFrame:
        df = self._preprocess(df)
        df = self._calculate_MA(df)
        df = self._format(df)
        return df


class Serializer(metaclass=ABCMeta):
    @abstractmethod
    def dump(self, df: SparkDataFrame) -> str:
        raise NotImplementedError


class SparkSerializer(Serializer):
    def dump(self, df: SparkDataFrame) -> str:
        data_dict = df.toJSON().map(json.loads).collect()
        json_str = json.dumps(data_dict)
        return json_str


class SparkUser(metaclass=ABCMeta):
    _spark: SparkSession


@dataclass
class RDDHandler:
    pipeline: Pipeline
    serializer: Serializer
    streaming_client: StreamingSender
    parent: SparkUser = None

    def handle(self, rdd: SparkRDD) -> None:
        if not rdd.isEmpty():
            df = self.parent._spark.read.json(rdd)
            df = self.pipeline.transform(df)
            data = self.serializer.dump(df).encode("utf-8")
            self.streaming_client.send_data(data)


class Streamer(SocketUser, SparkUser):
    def __init__(
        self,
        socket_config: dict,
        rdd_handler: RDDHandler,
        batch_interval: int = 60,
    ) -> None:
        super().__init__(socket_config)
        self.batch_interval = batch_interval
        self.rdd_handler = rdd_handler
        self.rdd_handler.parent = self

        self._spark = SparkSession.builder.appName("FinanceStreaming").getOrCreate()
        sc = self._spark.sparkContext
        self._ssc = StreamingContext(sc, self.batch_interval)

        dstream = self._ssc.socketTextStream(hostname=self.host, port=self.port)
        dstream.foreachRDD(lambda rdd: self.rdd_handler.handle(rdd))

    def start(self) -> None:
        self._ssc.start()
        self._ssc.awaitTermination()

    def stop(self) -> None:
        self._ssc.stop()
