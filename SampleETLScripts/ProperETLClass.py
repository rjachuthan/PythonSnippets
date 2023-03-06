import json
from typing import Any, Dict
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

# ============================================================================
# DataSource Class
# ============================================================================


class DataSource(ABC):
    """This is a Blueprint Class (Strategy) for creating new Data sources."""
    @abstractmethod
    def read(self, spark: SparkSession, input_path: str, options: Dict[str, Any]) -> 'SparkData':
        """This where you will write logic for reading different data formats."""
        pass


class CSVDataSource(DataSource):
    """This class is to read CSV data format."""

    def read(self, spark: SparkSession, input_path: str, options: Dict) -> 'SparkData':
        """Reading CSV files as parquet format.
        :param spark (SparkSession): Spark session
        :param input_path (str): Path to the CSV files
        :param options (Dict[str, Any]): Options to be passed for reading CSV format.

        :return SparkData
        """
        df = spark.read.format('csv').options(**options).load(input_path)
        return SparkData(df)


class JSONDataSource(DataSource):
    """This class is to read JSON data format."""

    def read(self, spark: SparkSession, input_path: str, options: Dict) -> 'SparkData':
        """Reading CSV files as parquet format.
        :param spark (SparkSession): Spark session
        :param input_path (str): Path to the CSV files
        :param options (Dict[str, Any]): Options to be passed for reading CSV format.

        :return SparkData
        """
        df = spark.read.format('json').options(**options).load(input_path)
        return SparkData(df)


class DeltaDataSource(DataSource):
    """This class is to read Delta Parquet data format."""

    def read(self, spark: SparkSession, input_path: str, options: Dict) -> 'SparkData':
        """Reading CSV files as parquet format.
        :param spark (SparkSession): Spark session
        :param input_path (str): Path to the CSV files
        :param options (Dict[str, Any]): Options to be passed for reading CSV format.

        :return SparkData
        """
        df = spark.read.format('delta').options(**options).load(input_path)
        return SparkData(df)

# ============================================================================
# Transformation Class
# ============================================================================


class Transformer(ABC):
    """This is a Blueprint Class (Strategy) for creating new types of Data Transformations."""
    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """This where you will write new logic for transforming the Spark Data."""
        pass


class RenameColumnsTransformer(Transformer):
    """Strategy to rename column names in a Spark dataframe."""

    def __init__(self, columns_map: dict):
        self.columns_map = columns_map

    def transform(self, df: DataFrame) -> DataFrame:
        """Transformation function for the Strategy.
        :param df (Spark DataFrame): The dataframe which is to be transformed.
        :return (Spark DataFrame): Transformed Spark Dataframe.
        """
        for old_col, new_col in self.columns_map.items():
            df = df.withColumnRenamed(old_col, new_col)
        return df


class FilterTransformer(Transformer):
    """Strategy to rename column names in a Spark dataframe."""

    def __init__(self, condition: str):
        self.condition = condition

    def transform(self, df: DataFrame) -> DataFrame:
        """Transformation function for the Strategy.
        :param df (Spark DataFrame): The dataframe which is to be transformed.
        :return (Spark DataFrame): Transformed Spark Dataframe.
        """
        return df.filter(self.condition)


class GroupByTransformer(Transformer):
    """Strategy to rename column names in a Spark dataframe."""

    def __init__(self, group_by_cols: list, agg_exprs: dict):
        self.group_by_cols = group_by_cols
        self.agg_exprs = agg_exprs

    def transform(self, df: DataFrame) -> DataFrame:
        """Transformation function for the Strategy.
        :param df (Spark DataFrame): The dataframe which is to be transformed.
        :return (Spark DataFrame): Transformed Spark Dataframe.
        """
        return df.groupBy(*self.group_by_cols).agg(self.agg_exprs)

# ============================================================================
# PySpark ETL Class
# ============================================================================


class SparkData:
    """Class object to performn ETL operation of various datasets."""

    def __init__(self, spark, config_path):
        self.spark = spark
        self.config = json.load(open(config_path))
        self.df = None

    @staticmethod
    def load(spark: SparkSession, input_path: str, data_format: str, options: Dict) -> 'SparkData':
        """Function to load any format of dataset to parquet format.
        :param spark (SparkSession): Spark session
        :param input_path (str): Path to the input files.
        :param data_format (str): Data format of the input files. (CSV, JSON, Delta, Parquet)
        :param options (Dict[str, Any]): Options to be passed while reading the input data.
        :return Spark DataFrame
        """
        data_sources = {
            'csv': CSVDataSource(),
            'json': JSONDataSource(),
            'delta': DeltaDataSource(),
        }
        return data_sources[data_format].read(spark, input_path, options)

    def transform(self, transformers: list[Transformer]):
        """Function which automatically performs all the transformations mentioned
        in the Config file.
        :param transfomers (list[Transformer]): Transformations on data
        :return (Spark DataFrame): Returns Transformed Dataset.
        """
        for transform in transformers:
            self.df = transform.transform(self.df)

    def write(self, output_path: str) -> None:
        """Function to write the Parquet file in Delta Format.
        :param output_path (str): Path to write the dataset.
        """
        self.df.write.mode("overwrite").format("delta").save(output_path)


if __name__ == "__main__":

    # Configuration for the Python ETL Job
    config = {
        "source_path": "source.csv",
        "schema": "col1 INT, col2 STRING, col3 DOUBLE",
        "transformers": [
            {
                "name": "RenameColumnsTransformer",
                "args": {"columns_map": {"col1": "id", "col2": "name", "col3": "price"}},
            },
            {
                "name": "FilterTransformer",
                "args": {"condition": "price > 10"},
            },
            {
                "name": "GroupByTransformer",
                "args": {"group_by_cols": ["name"], "agg_exprs": {"price": "sum"}},
            },
        ],
    }

    transformers = []
    for t in config["transformers"]:
        transformer_class = globals()[t["name"]]
        transformer = transformer_class(**t["args"])
        transformers.append(transformer)

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("spark-etl")
        .getOrCreate()
    )
    config_path = "<path to configuration JSON. Sample above.>"
    input_path = "<path to input files.>"
    output_path = "<path to where to store output files.>"

    spark_data = SparkData(spark, config_path)
    spark_data.load(
        input_path=input_path,
        data_format="delta",
        options={"header": "True"}
    )
    spark_data.transform(transformers)
    spark_data.write(output_path)
