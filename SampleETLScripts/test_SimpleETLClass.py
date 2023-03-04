import pytest
from pyspark.sql import SparkSession
from spark_data import SparkData


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("pytest-spark") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def data(spark):
    data = [
        (1, "A", 10),
        (2, "B", 20),
        (3, "C", 30),
        (4, "D", None),
        (5, None, None),
    ]
    columns = ["id", "name", "value"]
    return spark.createDataFrame(data, columns)


def test_load(data):
    spark_data = SparkData(data)
    path = "test_data.csv"
    format = "csv"
    options = {"header": True}
    spark_data.load(path, format, **options)
    # Assert loaded data has same count as original data
    assert spark_data.df.count() == data.count()  


def test_clean(data):
    spark_data = SparkData(data)
    spark_data.clean()
    # Assert dropped null values
    assert spark_data.df.count() == 3  

def test_transform(data):
    spark_data = SparkData(data)
    spark_data.transform()
    # Assert added new column
    assert spark_data.df.columns == ["id", "name", "value", "new_column"]  

def test_filter(data):
    spark_data = SparkData(data)
    spark_data.filter("value > 20")
    # Assert filtered data has correct count
    assert spark_data.df.count() == 2  

def test_group_by(data):
    spark_data = SparkData(data)
    spark_data.group_by("name")
    # Assert grouped data has only one partition
    assert spark_data.df.rdd.getNumPartitions() == 1  

def test_write(data, spark, tmp_path):
    spark_data = SparkData(data)
    path = tmp_path / "output_data"
    format = "delta"
    mode = "overwrite"
    spark_data.write(str(path), format, mode)
    loaded_data = spark.read.format(format).load(str(path))
    # Assert loaded data has same count as original data
    assert loaded_data.count() == data.count()  

