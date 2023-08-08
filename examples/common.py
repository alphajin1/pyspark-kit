def is_exist_path(sc, path):
    jvm = sc._jvm
    jsc = sc._jsc
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jsc.hadoopConfiguration())
    if fs.exists(jvm.org.apache.hadoop.fs.Path(path)):
        return True

    return False