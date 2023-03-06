import logging

class LoggerDecorator:
    def __init__(self, logger=None):
        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger

    def __call__(self, func):
        def wrapped_func(*args, **kwargs):
            self.logger.info(f"Input args: {args}")
            result = func(*args, **kwargs)
            self.logger.info(f"Output: {result}")
            return result
        return wrapped_func


class SparkData:
    def __init__(self, config_file):
        self.config = self.load_config(config_file)

    @LoggerDecorator()
    def read_delta_table(self, table_name):
        # code to read delta table

    @LoggerDecorator()
    def join_tables(self, table1, table2, on_cols):
        # code to join tables

    @LoggerDecorator()
    def filter_data(self, df, country, date_range):
        # code to filter data

    @LoggerDecorator()
    def transform_data(self, df, transformers):
        # code to transform data

    @LoggerDecorator()
    def write_delta_table(self, df, table_name):
        # code to write delta table
