# BigData_IDL1
# Proyecto ETL y Batch con Apache Spark

Este proyecto contiene dos scripts de Python que utilizan Apache Spark para realizar tareas de **ETL** (Extract, Transform, Load) y **Batch Processing**. Los scripts están diseñados para ser ejecutados en un entorno virtual para facilitar la gestión de dependencias.

## Requisitos

Antes de ejecutar los scripts, asegúrate de tener los siguientes requisitos instalados:

- **Python 3.x** (recomendado: 3.7+)
- **Apache Spark** (preferentemente con Hadoop)
- **Java 8 o superior** (para Spark)
- **pip** (gestor de paquetes de Python)
- **Entorno Virtual** (opcional pero recomendado)

## Instrucciones para el entorno virtual

Sigue estos pasos para configurar tu entorno virtual:

### 1. Crear un entorno virtual

Si no tienes un entorno virtual, puedes crear uno con el siguiente comando:

´´´bash
python -m venv venv
bash´´´
Esto creará un entorno virtual llamado venv. 

### 2. Activar el entorno virtual

_ Windows:

.\venv\Scripts\activate

_ Linux/macOS:

source venv/bin/activate

### 3. Instalar dependencias

Instala las dependencias necesarias para el proyecto ejecutando:

pip install -r requirements.txt

Asegúrate de que el archivo requirements.txt tenga las siguientes dependencias:

pyspark
pandas

### 4. Configurar Apache Spark
Asegúrate de tener Apache Spark configurado en tu máquina. Si no lo tienes, puedes seguir la guía de instalación oficial de Apache Spark: Documentación de Apache Spark.

También necesitarás configurar la variable de entorno SPARK_HOME para que apunte a la ubicación de Spark en tu sistema.


### Descripción de los Scripts
#### 1. ETL_data.py

Este script realiza el proceso de ETL. Extrae datos de un archivo CSV almacenado en HDFS, los transforma (limpieza de datos, agregación, etc.) y finalmente los carga nuevamente en HDFS.

Instrucciones de ejecución

python ETL_data.py

Este script realiza las siguientes acciones:

Carga los datos desde un archivo CSV en HDFS.
Realiza transformaciones de limpieza (eliminar nulos, duplicados, y conversiones de tipo de datos).
Realiza agregaciones y genera nuevas columnas.
Guarda los resultados procesados nuevamente en HDFS.

#### 2. Proc_batch.py

Este script ejecuta un procesamiento Batch sobre un conjunto de datos. Realiza un procesamiento periódico de los datos, y analiza las metricas de evaluacion.

Instrucciones de ejecución

python Proc_batch.py.py

Este script realiza las siguientes acciones:

Carga los datos desde HDFS o desde una fuente de datos configurada.

Guarda los resultados procesados nuevamente en HDFS.

#### Notas Importantes

Rendimiento: Ten en cuenta que el procesamiento en Spark puede requerir una cantidad significativa de recursos de memoria y CPU dependiendo del tamaño de los datos.
Configuración de Spark: Si estás trabajando en un entorno distribuido, asegúrate de tener configurado correctamente tu clúster de Spark.

#### Contribuciones
Si deseas contribuir a este proyecto, por favor sigue los siguientes pasos:

1. Haz un fork del repositorio.
2. Crea una nueva rama (git checkout -b feature/mi-nueva-funcionalidad).
3. Realiza tus cambios y haz un commit (git commit -am 'Agregada nueva funcionalidad').
4. Envía un pull request.
#### Licencia
Este proyecto está bajo la Licencia MIT. Ver el archivo LICENSE para más detalles.
