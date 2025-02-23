from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import DecisionTreeRegressor, LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.pipeline import Pipeline
from pyspark.sql.functions import col, count, when, isnan, stddev, mean, min,round
from pyspark.sql.functions import max as max_spark
# Iniciar sesion en Spark Optimizado
spark = SparkSession.builder.appName("PrediccionDePeso") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.shuffle.partitions", "50") \
    .getOrCreate()

# Cargar el archivo desde HDFS
ruta_hdfs = "hdfs://localhost:9000/BigData/DatosPrediccionPeso_Ok.csv"

try:

    df_spark = spark.read.csv(ruta_hdfs, header=True, inferSchema=True, sep=";")

    # Caché si se usará varias veces
    df_spark.cache()

    # Persistir en memoria y disco si es muy grande
    df_spark.persist()

    # Mostrar primeros datos
    df_spark.show(5)

    # Validacion 1: Verificar valores nulos
    print("\nVerificando valores nulos por columna:")
    df_spark.select([count(when(col(c).isNull(), c)).alias(c) for c in df_spark.columns]).show()
    print("\nEliminando valores nulos:")
    df_spark = df_spark.na.drop()
    df_spark.select([count(when(col(c).isNull(), c)).alias(c) for c in df_spark.columns]).show()

    # Validacion 2: Verificar duplicados
    print("\nCantidad de filas duplicadas:")
    duplicates = df_spark.count() - df_spark.dropDuplicates().count()
    print(f"Filas duplicadas: {duplicates}")

    # Validacion 3: Verificar tipos de datos
    print("\nEsquema del DataFrame:")
    df_spark.printSchema()

    # Validacion 4: Estadisticas generales (para detectar valores anomalos)
    print("\nEstadisticas generales de las columnas numericas:")
    numeric_cols = [c[0] for c in df_spark.dtypes if c[1] in ['int', 'double', 'float']]
    if numeric_cols:
        # Calcular la media de las columnas numéricas
        mean_values = df_spark.select([round(mean(c),3).alias(f"{c}_mean") for c in numeric_cols])
        # Convertir el DataFrame de una fila a múltiples filas (transponer)
        mean_df = mean_values.selectExpr(
            "stack({}, {}) as (Columna, Media)".format(
                len(numeric_cols),
                ", ".join([f"'{c}_mean', `{c}_mean`" for c in numeric_cols])
            )
        )
        # Mostrar el resultado Media
        mean_df.show(truncate=False)

        #Calcular la Desviacion Standar 
        std_values = df_spark.select([round(stddev(c),3).alias(f"{c}_stddev") for c in numeric_cols])
        # Convertir el DataFrame de una fila a múltiples filas (transponer)
        std_df = std_values.selectExpr(
            "stack({}, {}) as (Columna, Desviacion_Estandar)".format(
                len(numeric_cols),
                ", ".join([f"'{c}_stddev' , `{c}_stddev`" for c in numeric_cols])
            )
        )
        #Mostrar el resultado Std
        std_df.show(truncate=False)

        # Convertir todas las columnas numéricas a DOUBLE
        df_spark = df_spark.select(
            [col(c).cast("double").alias(c) for c in numeric_cols]
)
        #Minimo Valor de cada columna
        min_values = df_spark.select([min(c).alias(f"{c}_min") for c in numeric_cols])
        # Convertir el DataFrame de una fila a múltiples filas (transponer)
        min_df = min_values.selectExpr(
            "stack({}, {}) as (Columna, Valor_Minimo)".format(
                len(numeric_cols),
                ", ".join([f"'{c}_min' , `{c}_min`" for c in numeric_cols])
            )
        )
        #Mostrar el resultado Min
        min_df.show(truncate=False)

        #Maximo valor de cada columna
        max_values = df_spark.select([max_spark(c).alias(f"{c}_max") for c in numeric_cols])
        # Convertir el DataFrame de una fila a múltiples filas (transponer)
        max_df = max_values.selectExpr(
            "stack({}, {}) as (Columna, Valor_Maximo)".format(
                len(numeric_cols),
                ", ".join([f"'{c}_max' , `{c}_max`" for c in numeric_cols])
            )
        )
        #Mostrar el resultado Max
        max_df.show(truncate=False)

    else:
        print("No hay columnas numericas en los datos.")

    # Definir las caracteristicas y la etiqueta
    feature_cols = ["PesoSem4", "Agua", "PesoSem3", "ConsumoAcabado", "MortStd"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

    df_spark = assembler.transform(df_spark).select("features", "PesoFinal")

    # Dividir los datos en entrenamiento y prueba
    train_data, test_data = df_spark.randomSplit([0.7, 0.3], seed=42)

    # Definir los modelos
    models = {
        "decision_tree": DecisionTreeRegressor(featuresCol="features", labelCol="PesoFinal", maxDepth=5),
        "linear_regression": LinearRegression(featuresCol="features", labelCol="PesoFinal"),
        "random_forest": RandomForestRegressor(featuresCol="features", labelCol="PesoFinal", numTrees=10, maxDepth=6, minInstancesPerNode=3),
        "gradient_booster": GBTRegressor(featuresCol="features", labelCol="PesoFinal", maxIter=10)
    }

    # Entrenar y evaluar modelos
    predictions = {}
    evaluator = RegressionEvaluator(labelCol="PesoFinal", predictionCol="prediction", metricName="r2")

    for name, model in models.items():
        pipeline = Pipeline(stages=[model])
        trained_model = pipeline.fit(train_data)
        pred = trained_model.transform(test_data)
        r2 = evaluator.evaluate(pred)
        predictions[name] = (trained_model, r2)
        print(f"{name}: R2 = {r2:.4f}")

    # Seleccionar el mejor modelo
    best_model = max(predictions, key=lambda k: predictions[k][1])
    print(f"El mejor modelo es {best_model} con R2 = {predictions[best_model][1]:.3f}")

    # Obtener el modelo entrenado
    trained_model = predictions[best_model][0]

    # Hacer predicciones en todo el dataset
    df_spark = trained_model.transform(df_spark)

    # Redondear solo la columna "prediction" a 3 decimales
    df_spark = df_spark.withColumn("prediction", round(col("prediction"), 3))

    # Mostrar las columnas "PesoFinal" y "prediction"
    df_spark.select("PesoFinal", "prediction").show()

    # Crear un evaluador de regresión
    evaluator = RegressionEvaluator(labelCol="PesoFinal", predictionCol="prediction")

    # Calcular las métricas de rendimiento en el DataFrame con las predicciones
    mse = evaluator.evaluate(df_spark, {evaluator.metricName: "mse"})  # Error Cuadrático Medio
    rmse = evaluator.evaluate(df_spark, {evaluator.metricName: "rmse"})  # Raíz del Error Cuadrático Medio
    mae = evaluator.evaluate(df_spark, {evaluator.metricName: "mae"})  # Error Absoluto Medio

    # Mostrar las métricas
    print(f"Error Cuadratico Medio (MSE): {mse:.3f}")
    print(f"Raiz del Error Cuadratico Medio (RMSE): {rmse:.3f}")
    print(f"Error Absoluto Medio (MAE): {mae:.3f}")
    # Detener Spark
    spark.stop()

except Exception as e:
    print("\nError al cargar y validar datos desde HDFS:\n", str(e).encode("utf-8", "ignore").decode("utf-8"))

