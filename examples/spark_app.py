from pyspark.sql import SparkSession

def create_spark_session(name):
    spark = SparkSession \
        .builder \
        .appName(name) \
        .getOrCreate()

    sc = spark.sparkContext

    log4jLogger = sc._jvm.org.apache.log4j
    log4jLogger.LogManager.getLogger("org").setLevel(log4jLogger.Level.WARN)
    log4jLogger.LogManager.getLogger("akka").setLevel(log4jLogger.Level.WARN)
    LOGGER = log4jLogger.LogManager.getLogger(__name__)
    sc.setLogLevel('WARN')
    LOGGER.warn("===== SparkConfiguration =====")
    LOGGER.warn(spark.sparkContext.getConf().toDebugString())
    LOGGER.warn("===== END =====")

    logger = LOGGER
    print(sc.getConf().getAll())

    return spark, sc, logger

