from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class Transformer(ABC):
    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        pass


class RenameColumnsTransformer(Transformer):
    def __init__(self, columns_map: dict):
        self.columns_map = columns_map

    def transform(self, df: DataFrame) -> DataFrame:
        for old_col, new_col in self.columns_map.items():
            df = df.withColumnRenamed(old_col, new_col)
        return df


class FilterTransformer(Transformer):
    def __init__(self, condition: str):
        self.condition = condition

    def transform(self, df: DataFrame) -> DataFrame:
        return df.filter(self.condition)


class GroupByTransformer(Transformer):
    def __init__(self, group_by_cols: list, agg_exprs: dict):
        self.group_by_cols = group_by_cols
        self.agg_exprs = agg_exprs

    def transform(self, df: DataFrame) -> DataFrame:
        return df.groupBy(*self.group_by_cols).agg(self.agg_exprs)


class SparkData:
    def __init__(self, spark, config_path):
        self.spark = spark
        self.config = json.load(open(config_path))
        self.df = None

    def load(self):
        source_path = self.config["source_path"]
        schema = self.config["schema"]
        self.df = self.spark.read.csv(source_path, header=True, schema=schema)

    def transform(self, transformers: list):
        for transformer in transformers:
            self.df = transformer.transform(self.df)

    def write(self, output_path):
        self.df.write.mode("overwrite").format("delta").save(output_path)


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

spark_data = SparkData(spark, config_path)
spark_data.load()
spark_data.transform(transformers)
spark_data.write(output_path)

