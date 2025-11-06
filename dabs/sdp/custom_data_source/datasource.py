from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    SimpleDataSourceStreamReader,
)
from pyspark.sql.types import StructType, IntegerType, DateType, StringType
from typing import Iterator, Tuple


class FakeDataSource(DataSource):
    """
    An example data source for batch and streaming query using the `faker` library.
    """

    @classmethod
    def name(cls):
        return "fake"

    def schema(self):
        return "id int, name string, date string, zipcode string, state string"

    def reader(self, schema: StructType):
        """For batch reads"""
        return FakeDataSourceReader(schema, self.options)

    def simpleStreamReader(self, schema: StructType):
        """For streaming reads - simpler, no partitioning"""
        return FakeSimpleStreamReader(schema, self.options)


class FakeDataSourceReader(DataSourceReader):
    """Batch reader implementation"""

    def __init__(self, schema, options):
        self.schema: StructType = schema
        self.options = options

    def _generate_value(self, field, fake, row_index=0):
        field_name = field.name
        field_type = field.dataType

        # Generate ID from row index
        if field_name == "id":
            return row_index

        if isinstance(field_type, IntegerType):
            return fake.pyint(min_value=1, max_value=100000)
        elif isinstance(field_type, DateType):
            return fake.date_object()
        elif isinstance(field_type, StringType):
            if hasattr(fake, field_name):
                return str(getattr(fake, field_name)())
            else:
                return fake.word()
        return None

    def read(self, partition):
        from faker import Faker

        fake = Faker()
        num_rows = int(self.options.get("numRows", 3))
        for i in range(num_rows):
            row = []
            for field in self.schema.fields:
                value = self._generate_value(field, fake, row_index=i)
                row.append(value)
            yield tuple(row)


class FakeSimpleStreamReader(SimpleDataSourceStreamReader):
    """Simple streaming reader - generates fake data in each microbatch"""

    def __init__(self, schema, options):
        self.schema: StructType = schema
        self.options = options
        self.max_offset = int(
            options.get("maxRows", 1000)
        )  # Stop after 1000 total rows

    def _generate_value(self, field, fake, row_id):
        field_name = field.name
        field_type = field.dataType

        if field_name == "id":
            return row_id

        if isinstance(field_type, IntegerType):
            return fake.pyint(min_value=1, max_value=100000)
        elif isinstance(field_type, DateType):
            return fake.date_object()
        elif isinstance(field_type, StringType):
            if hasattr(fake, field_name):
                return str(getattr(fake, field_name)())
            else:
                return fake.word()
        return None

    def initialOffset(self):
        """Returns the initial start offset of the reader."""
        return {"offset": 0}

    def read(self, start: dict) -> (Iterator[Tuple], dict):
        """
        Takes start offset as input, returns an iterator of tuples
        and the start offset of the next read.
        """
        from faker import Faker
        import time

        fake = Faker()
        time.sleep(5)
        start_offset = start["offset"]

        if start_offset >= self.max_offset:
            return (iter([]), start)  # Return empty iterator - no more data

        num_rows = int(self.options.get("numRows", 2))  # Generate 2 rows per microbatch

        rows = []
        for i in range(num_rows):
            row_id = start_offset + i
            row = []
            for field in self.schema.fields:
                value = self._generate_value(field, fake, row_id)
                row.append(value)
            rows.append(tuple(row))

        next_offset = {"offset": start_offset + num_rows}
        return (iter(rows), next_offset)

    def readBetweenOffsets(self, start: dict, end: dict) -> Iterator[Tuple]:
        """
        Reads deterministically between offsets.
        Called when query replays batches during restart or after failure.
        """
        from faker import Faker

        fake = Faker()
        fake.seed_instance(start["offset"])  # Seed for deterministic replay

        start_idx = start["offset"]
        end_idx = end["offset"]
        num_rows = end_idx - start_idx

        for i in range(num_rows):
            row = []
            for field in self.schema.fields:
                value = getattr(fake, field.name)()
                row.append(value)
            yield tuple(row)

    def commit(self, end):
        """Invoked when query finishes processing data before end offset."""
        pass

