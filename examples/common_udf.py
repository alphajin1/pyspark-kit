from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, BooleanType, StructField
import re


@F.udf(StringType())
def normalize_query(query):
    query = re.sub('\s', '', query).lower()
    query = re.sub('\n', '', query)
    return re.sub(r'[\x00-\x1F]+', '', query)


@F.udf(StructType([
    StructField("a", StringType(), False),
    StructField("b", BooleanType(), False),
])
)
def transform_code(x, y):
    return "alphabet", True
