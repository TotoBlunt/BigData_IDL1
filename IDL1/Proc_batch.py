import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when
import psutil
# 🔹 Iniciar sesión de Spark
spark = SparkSession.builder \
    .appName("Batch Processing - Sales Data") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

# 🕒 Registrar tiempo de inicio
start_time = time.time()


try:
    print("📌 Iniciando procesamiento batch...")

    # 🔹 1. Cargar datos desde HDFS (Extracción)
    file_path = "hdfs://localhost:9000/user/BigDataToto/datos/sales_data_IDL1.csv"
    sales_data = spark.read.csv(file_path, header=True, inferSchema=True)
    print("✅ Datos cargados desde HDFS.")

    # 🔹 2. Transformaciones
    ventas = sales_data.dropna().dropDuplicates()
    ventas = ventas.withColumn("Date", to_date(col("Date")))
    ventas = ventas.withColumn(
        "Categoria_Venta",
        when(col("Sales") > 1000, "Alta")
        .when(col("Sales").between(500, 1000), "Media")
        .otherwise("Baja")
    )
    print("🔄 Transformaciones completadas.")

    # 🔹 3. Guardar los resultados en HDFS (Load)
    output_path = "hdfs://localhost:9000/user/BigDataToto/datos/ventas_procesadas"
    ventas.write.parquet(output_path, mode="overwrite")
    print("✅ Datos procesados guardados en HDFS.")

    # 🕒 Calcular tiempo de ejecución
    execution_time = time.time() - start_time
    print(f"⏳ Tiempo de ejecución del batch: {execution_time:.2f} segundos")
    # Obtener uso de CPU y memoria
    cpu_usage = psutil.cpu_percent(interval=1)
    ram_usage = psutil.virtual_memory().percent
    print(f"🔍 Uso de CPU: {cpu_usage}%")
    print(f"🔍 Uso de RAM: {ram_usage}%")

except Exception as e:
    print(f"❌ Error en el procesamiento batch: {str(e)}")

finally:
    spark.stop()
    print("📌 Sesión de Spark finalizada.")
