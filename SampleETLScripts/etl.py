from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dataclasses import dataclass


@dataclass
class Customer:
    id: int
    name: str
    address: str


@dataclass
class Order:
    id: int
    customer_id: int
    order_date: str
    total: float


class ETLJob:
    def __init__(self, spark, source_dir, output_dir):
        self.spark = spark
        self.source_dir = source_dir
        self.output_dir = output_dir

    def load_data(self):
        customers_df = self.spark.read.csv(self.source_dir + "/customers.csv", header=True, inferSchema=True)
        orders_df = self.spark.read.csv(self.source_dir + "/orders.csv", header=True, inferSchema=True)

        return customers_df, orders_df

    def combine_data(self, customers_df, orders_df):
        customers = customers_df.rdd.map(lambda row: Customer(row[0], row[1], row[2])).collect()
        orders = orders_df.rdd.map(lambda row: Order(row[0], row[1], row[2], row[3])).collect()

        customer_dict = {c.id: c for c in customers}

        joined_df = orders_df.rdd.map(lambda row: (
            row[0], customer_dict[row[1]].name, customer_dict[row[1]].address, row[2], row[3]
        )).toDF(["order_id", "customer_name", "customer_address", "order_date", "total"])

        return joined_df

    def transform_data(self, joined_df):
        transformed_df = joined_df.withColumn("year", year(col("order_date"))) \
            .groupBy("year", "customer_name") \
            .agg(sum("total").alias("total_sales")) \
            .filter(col("total_sales") > 1000)

        return transformed_df

    def write_data(self, transformed_df):
        transformed_df.write.format("delta").mode("append").save(self.output_dir)

    def run(self):
        customers_df, orders_df = self.load_data()
        joined_df = self.combine_data(customers_df, orders_df)
        transformed_df = self.transform_data(joined_df)
        self.write_data(transformed_df)


# Example usage:
spark = SparkSession.builder.appName("myApp").getOrCreate()
etl_job = ETLJob(spark, "/path/to/source/data", "/path/to/output/data")
etl_job.run()


