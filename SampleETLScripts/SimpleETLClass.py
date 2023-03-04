import great_expectations as ge
from pyspark.sql import DataFrame

class SparkData:
    def __init__(self, df: DataFrame):
        self.df = df
    
    def load(self, path: str, format: str = "csv", **options):
        self.df = self.df.read.format(format).options(**options).load(path)
        return self
    
    def clean(self):
        # Perform data cleaning operations
        # e.g. drop null values, fill missing values, etc.
        self.df = self.df.dropna()
        return self
    
    def transform(self):
        # Perform data transformation operations
        # e.g. adding new columns, converting data types, etc.
        self.df = self.df.withColumn("new_column", self.df["old_column"] + 1)
        return self
    
    def filter(self, condition):
        # Filter the data based on a condition
        self.df = self.df.filter(condition)
        return self
    
    def group_by(self, *cols):
        # Group the data by one or more columns
        self.df = self.df.groupBy(*cols)
        return self
    
    def write(self, path: str, format: str = "delta", mode: str = "overwrite", **options):
        self.df.write.format(format).mode(mode).options(**options).save(path)
        return self

    def validate(self):
        suite = ge.dataset.SparkDFValidationSuiteBuilder(self.df).build()
        results = suite.validate()
        for result in results:
            if not result["success"]:
                raise AssertionError(result["result"])

if __name__ = "__main__":
    spark_data = SparkData(spark.read)
    spark_data.load("input_data.csv", format="csv", header=True)
    spark_data.clean()
    spark_data.transform()
    spark_data.filter("column_name > 0")
    spark_data.group_by("column_name")
    spark_data.validate()
    spark_data.write("output_data", format="delta", mode="overwrite")

