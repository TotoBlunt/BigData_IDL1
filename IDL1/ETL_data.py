from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date,when
from pyspark.sql.window import Window

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("Sales Analysis") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

# Cargar el archivo CSV desde HDFS (Extracción)
file_path = "hdfs://localhost:9000/user/BigDataToto/datos/sales_data_IDL1.csv"
sales_data = spark.read.csv(file_path, header=True, inferSchema=True)

# Mostrar los datos cargados
print("Datos cargados:")
sales_data.show()

# Mostrar dimensiones del DataFrame
num_filas = sales_data.count()
num_columnas = len(sales_data.columns)
print(f"Columnas: {num_columnas}")
print(f"Filas: {num_filas}")

# Transformaciones (Transform)
# Eliminar valores nulos
ventas = sales_data.dropna()

# Eliminar duplicados
ventas = ventas.dropDuplicates()

# Transformar a formato fecha si no lo está
ventas = ventas.withColumn("Date", to_date(col("Date")))

#Generar columnas de indicador de ventas alta, media , baja
ventas = ventas.withColumn(
    "Categoria_Venta",
    when(col("Sales") > 1000, "Alta")
    .when(col("Sales").between(500, 1000), "Media")
    .otherwise("Baja")
)

ventas.show()

# Guardar los resultados en HDFS (Load)
output_path = "hdfs://localhost:9000/user/BigDataToto/datos/ventas_limpio.csv"
ventas.write.csv(output_path, header=True, mode="overwrite")

# Cerrar la sesión de Spark
spark.stop()
