# PySpark

Pyspark es una herramienta para *Cluste Computing*. Permite separar la data y computaciones entre *clusters* con multiples *nodos* (Maybe computadoras).

En práctica el cluster hosteará una maquina remota que está conectada a todos los nodos. Habrá una computadora llamada *master* que estará conectada al resto de nodos (*workers*) en el cluster.

## Conceptos

- SparkContext. Conexión con el cluster.
  - SparkConf(): Crear objeto de conexión.

- RDD - Resilient Dristributed Data. Objeto de bajo nivel
- DataFrame - Abstracción de RDD que se asemeja a una tabla de SQL. Están oprimizadas.

- SparkSession - Interfaz con la conexión al cluster (SparkContext).

- Partitioning. Spark trabaja separando la data en particiones.
- Lazy processing. Toda operación de transformacion.

## SparkSession

- Crear Spark Session
  
```python
from pyspqrk.sql import SparkSession

my_spark = SpqrkSession.builder.getOrCreate()

```

- Catalog - Atributo de Spark Session para ver la data que se encuentra dentro del cluster.

```Python
spark.catalog.listTables()
```

# DataFrame

Permite correr querys de SQL.

- Método sql 

```Python
dataFrame = spark_session.sql(query)
dataFrame.show()
```

- Pandafy a DataFrame

```Python
pd_df = pySparkDF.toPandas()
```

- Sparkfy a Pandas Df

La salidad está guardada localmente, no en el SparkSession. 
Si queremos hacer una query con la nueva data habrá un error. Para evitarlo tendremos que crear un *temporary table*
La temporaryTable solo puede ser accesada por la Session que la creó.

```python
# Create pd_temp
pd_temp = pd.DataFrame(np.random.random(10))

# Create spark_temp from pd_temp
spark_temp = spark.createDataFrame(pd_temp)

# Add spark_temp to the catalog
spark_temp.createOrReplaceTempView('temp')

```

- Leer archivos
```python
airports = spark.read.csv(file_path, header=True)
```
- Columnas
  
Reciben dos argumentos, un *string* con el nombre y la columna.
  
La nueva columna debe ser un objeto de tipo *Column*. Se puede extraer una columna del DataFrame usando *df.colName*.

Un Spark DataFrame es inmutable.

Todos estos métodos regresan un nuevo DataFrame para sobreescribir el viejo.

```Python 
df = df.withColumn('newCol', df.oldCol + 1)
```

```Python
flights = spark.table('flights')

flights = flights.withColumn('duration_hrs', flights.air_time / 60)
```

Drop columnas
```Python
df = df.drop(df.colName)
```

```Python
# Load the CSV file
aa_dfw_df = spark.read.format('csv').options(Header=True).load('AA_DFW_2018.csv.gz')

# Add the airport column using the F.lower() method
aa_dfw_df = aa_dfw_df.withColumn('airport', F.lower(aa_dfw_df['Destination Airport']))

# Drop the Destination Airport column
aa_dfw_df = aa_dfw_df.drop(aa_dfw_df['Destination Airport'])

# Show the DataFrame
aa_dfw_df.show()
```

- Filtrar data

Operación *.filter()* es analogo al *WHERE* en una consulta de SQL. Recibe una expresión *booleana*.

Puede recibir un string o una expresión de columna.

```Python
flights.filter("air_time > 120").show()
flights.filter(flights.air_time > 120).show()
```

La operación *.select()* es el analogo a *SELECT* en SQL. Recibe varios argumentos, uno por cada columna que queramos seleccionar.

Puede ser uns string o un objeto de tipo Column. Se pueden hacer operaciones si recibe un objeto de tipo Column.

*Select()* vs *withColumn()*
Select retorna solo las columnas que seleccionas y withColumn regresa todas las columnas más la que estás creando.

```python
# Select the first set of columns
selected1 = flights.select('tailnum', 'origin', 'dest')

# Select the second set of columns
temp = flights.select(flights.origin, flights.dest, flights.carrier)

# Define first filter
filterA = flights.origin == "SEA"

# Define second filter
filterB = flights.dest == "PDX"

# Filter the data, first by filterA then by filterB
selected2 = temp.filter(filterA).filter(filterB)
```

Select también puede realizar cualquier operación de coolumna usando el df.colName, la operación retornará la columna modificada.

```python
flights.select(flights.air_time/60)
```
Regresa la columna modificada, también es posible brindar un alias a las columnas que modifiquen.

```Python
flights.select((flights.air_time/60).alias("duration_hrs"))
```

Similar existe el método *selectExpr* que recibe una expresión de tipo SQL.

```python
flights.selectExpr("air_time/60 as duration_hrs")
```

```python
# Define avg_speed
avg_speed = ((flights.distance/(flights.air_time/60)).alias("avg_speed"))

# Select the correct columns
speed1 = flights.select("origin", "dest", "tailnum", avg_speed)

# Create the same table using a SQL expression
speed2 = flights.selectExpr("origin", "dest", "tailnum", "distance/(air_time/60) as avg_speed")

speed2.show()
```

- Aggregating

*.min()*, *.max()*, *.count()* son metodos de *GropuedData*. Son creados llamando a *.groupBy()* método del DataFrame.

```Python
df.groupBy().min("col").show()

# Find the shortest flight from PDX in terms of distance
flights.filter(flights.origin == 'PDX').groupBy().min('distance').show()

# Find the longest flight from SEA in terms of air time
flights.filter(flights.origin == 'SEA').groupBy().max('air_time').show()

flights.filter(flights.carrier == 'DL').filter(flights.origin == 'SEA').groupBy().avg('air_time').show()

# Total hours in the air
flights.withColumn("duration_hrs", flights.air_time/60).groupBy().sum("duration_hrs").show()
```

- Grouping and aggregating

Dentro del método .groupBy() se llama columnas y se comporta como las sentencias seguidas de *GROUP BY* en SQL.

```Python
# Group by tailnum
by_plane = flights.groupBy('tailnum')

# Number of flights each plane made
by_plane.count().show()

# Group by origin
by_origin = flights.groupBy('origin')

# Average duration of flights from PDX and SEA
by_origin.avg("air_time").show()
```

- Joins

Se usa el método del DataFrame llamado *.join()*.

Toma tres argumentos: el segundo dataFrame donde harás el join, el segundo es el nombre de la key column. Los nombres de la columna deberán ser los mismos. El tercero es *how* el tipo de join que se ejecutará.

```python
# Examine the data
#print(airports)
airports.show()
# Rename the faa column
airports = airports.withColumnRenamed('faa', 'dest')

# Join the DataFrames
flights_with_airports = flights.join(airports, 'dest', 'leftouter')

# Examine the new DataFrame
#print(flights_with_airports)
flights_with_airports.show()
```

## Cleaning data with python

Preparar la información para su uso en pipelines de procesamiento.

Posibles tareas:
- Reformar o reemplazar texto
- Realizar calculos
- Remover basura o datos incompletos

Spark es escalable y poderoso para el manejo de información.

Definir un esquema en Pyspark

```Python
from pyspark.sql.types import *

peopleSchema = StructType([
  # Nombre, tipo y si es null o no
  StructField('name', StringType(), True),
  StructField('age', IntegerType(), True),
  StructField('city', StringType(), True),
])

people_df = spark.read.format('csv').load(name='rawdata.csv', schema=peopleSchema)

```

## Parquet

Formato de columnas comprimido desarrollado para usarse en cualquier sistema Hadoop.

Un Parquet es un archivo binario. 

Leer una archivo parquet
```Python
df = spark.read.format('parquet').load('filename.parquet')
df = spark.read.parquet('filename.parquet')
```
Escribir un archivo parquet
```Python
df.write.format('parquet').save('filename.parquet')
df = spark.write.parquet('filename.parquet')
```

Parquet y SQL

```python
df = sparl.read.parquet('filename.parquet')
df.createOrReplaceTempView('column')
df_2 = spark.sql('query from column')
```

Union de DataFrames
```Python
df3 = df1.union(df2)
```

Escribir Parquet files
```python
# View the row count of df1 and df2
print("df1 Count: %d" % df1.count())
print("df2 Count: %d" % df2.count())

# Combine the DataFrames into one
df3 = df1.union(df2)

# Save the df3 DataFrame in Parquet format
df3.write.parquet('AA_DFW_ALL.parquet', mode='overwrite')

# Read the Parquet file into a new DataFrame and run a count
print(spark.read.parquet('AA_DFW_ALL.parquet').count())
```

## Filterin Data

- Remover valores nulos
- Remover entradas inusuales
- Split data

Column string transformations
 ```python
 import pyspark.sql.transformation as F
 ```
- upper
- split

```python
# Show the distinct VOTER_NAME entries
voter_df.select('VOTER_NAME').distinct().show(40, truncate=False)

# Filter voter_df where the VOTER_NAME is 1-20 characters in length
voter_df = voter_df.filter('length(VOTER_NAME) > 0 and length(VOTER_NAME) < 20')

# Filter out voter_df where the VOTER_NAME contains an underscore
voter_df = voter_df.filter(~ F.col('VOTER_NAME').contains('_'))

# Show the distinct VOTER_NAME entries again
voter_df.select('VOTER_NAME').distinct().show(40, truncate=False)
```
- Split
- 
```python
# Add a new column called splits separated on whitespace
voter_df = voter_df.withColumn('splits', F.split(voter_df.VOTER_NAME, '\s+'))

# Create a new column called first_name based on the first item in splits
voter_df = voter_df.withColumn('first_name', voter_df.splits.getItem(0))

# Get the last entry of the splits list and create a column called last_name
voter_df = voter_df.withColumn('last_name', voter_df.splits.getItem(F.size('splits') - 1))

# Drop the splits column
voter_df = voter_df.drop('splits')

# Show the voter_df DataFrame
voter_df.show()
```

## Conditional Clauses

Funcionan como *if* / *then* / *else*. Se puede usar instrucciones *if* pero esto es de bajo performance porque cada valor se evalua independientemente.

```python
.when(<if condition>, <then x>)
df.select(df.Name, df.Age, F.when(df.Age >= 18, 'Adult'))
```

| Name | Age | |
|---|---|---|
|Alan|22|Adult|
|Alan|17||
|Alan|23|Adult|

```Python
# Add a column to voter_df for a voter based on their position
voter_df = voter_df.withColumn('random_val',
                               when(voter_df.TITLE == 'Councilmember', F.rand())
                               .when(voter_df.TITLE == 'Mayor', 2)
                               .otherwise(0))

# Show some of the DataFrame rows
voter_df.show()

# Use the .filter() clause with random_val
voter_df.filter(voter_df.random_val == 0).show()
```

## UDFs: User defined functions

Es un método de python que puede ser mapeado a *pyspark.sql.functions.udf*.
Despues se puede usar como cualquier función de pyspark.

1. Definir una función en python

```Python
def reverseString(s):
  return s[::-1]
```

2. Wrap and store the function
```Python
udf_reverseStrig = udf(reverseString, StringType())
```

3. Use the function
```Python
def getFirstAndMiddle(names):
  # Return a space separated string of names
  return ' '.join(names)

# Define the method as a UDF
udfFirstAndMiddle = F.udf(getFirstAndMiddle, StringType())

# Create a new column using your UDF
voter_df = voter_df.withColumn('first_and_middle_name', udfFirstAndMiddle(voter_df.splits))

# Show the DataFrame
voter_df.show()
```

## Generar IDs en Spark

Un ID está bien para un único servidor pero como en spark se particiona la información es mejor generar IDs dentro de spark para no tener cuellos de botella.

```Python
# Select all the unique council voters
voter_df = df.select(df["VOTER NAME"]).distinct()

# Count the rows in voter_df
print("\nThere are %d rows in the voter_df DataFrame.\n" % voter_df.count())

# Add a ROW_ID
voter_df = voter_df.withColumn('ROW_ID', F.monotonically_increasing_id())

# Show the rows with 10 highest IDs in the set
voter_df.orderBy(voter_df.ROW_ID.desc()).show(10)
```

```Python
# Print the number of partitions in each DataFrame
print("\nThere are %d partitions in the voter_df DataFrame.\n" % voter_df.rdd.getNumPartitions())
print("\nThere are %d partitions in the voter_df_single DataFrame.\n" % voter_df_single.rdd.getNumPartitions())

# Add a ROW_ID field to each DataFrame
voter_df = voter_df.withColumn('ROW_ID', F.monotonically_increasing_id())
voter_df_single = voter_df_single.withColumn('ROW_ID', F.monotonically_increasing_id())

# Show the top 10 IDs in each DataFrame 
voter_df.orderBy(voter_df.ROW_ID.desc()).show(10)
voter_df_single.orderBy(voter_df_single.ROW_ID.desc()).show(10)
```

Empezar IDs desde el máximo de una lista anterior
```Python
# Determine the highest ROW_ID and save it in previous_max_ID
previous_max_ID = voter_df_march.select('ROW_ID').rdd.max()[0]

# Add a ROW_ID column to voter_df_april starting at the desired value
voter_df_april = voter_df_april.withColumn('ROW_ID', F.monotonically_increasing_id() + previous_max_ID)

# Show the ROW_ID from both DataFrames and compare
voter_df_march.select('ROW_ID').show()
voter_df_april.select('ROW_ID').show()
```

## Caching

Guardar DataFrames en memoria o disco. Mejora la velocidad en transacciones/acciones futuras.

Sin embargo, información de grandes volumenes puede no caber dentro del disco

```Python
start_time = time.time()

# Add caching to the unique rows in departures_df
departures_df = departures_df.cache().distinct()

# Count the unique rows in departures_df, noting how long the operation takes
print("Counting %d rows took %f seconds" % (departures_df.count(), time.time() - start_time))

# Count the rows again, noting the variance in time of a cached DataFrame
start_time = time.time()
print("Counting %d rows again took %f seconds" % (departures_df.count(), time.time() - start_time))
```

Removing from cache

```Python
# Determine if departures_df is in the cache
print("Is departures_df cached?: %s" % departures_df.is_cached)
print("Removing departures_df from cache")

# Remove departures_df from the cache
departures_df.unpersist()

# Check the cache status again
print("Is departures_df cached?: %s" % departures_df.is_cached)
```
## Spark Clusters
- Driver process
- Worker procesess

```Python
# Import the full and split files into DataFrames
full_df = spark.read.csv('departures_full.txt.gz')
split_df = spark.read.csv('departures_0*.txt.gz')

# Print the count and run time for each DataFrame
start_time_a = time.time()
print("Total rows in full DataFrame:\t%d" % full_df.count())
print("Time to run: %f" % (time.time() - start_time_a))

start_time_b = time.time()
print("Total rows in split DataFrame:\t%d" % split_df.count())
print("Time to run: %f" % (time.time() - start_time_b))
```

## Leyendo configuraciones de spark

```Python
# Name of the Spark application instance
app_name = spark.conf.get('spark.app.name')

# Driver TCP port
driver_tcp_port = spark.conf.get('spark.driver.port')

# Number of join partitions
num_partitions = spark.conf.get('spark.sql.shuffle.partitions')

# Show the results
print("Name: %s" % app_name)
print("Driver TCP port: %s" % driver_tcp_port)
print("Number of partitions: %s" % num_partitions)
```

Using the spark.conf object allows you to validate the settings of a cluster without having configured it initially. This can help you know what changes should be optimized for your needs.

### Configure 

```python
# Store the number of partitions in variable
before = departures_df.rdd.getNumPartitions()

# Configure Spark to use 500 partitions
spark.conf.set('spark.sql.shuffle.partitions', 500)

# Recreate the DataFrame using the departures data file
departures_df = spark.read.csv('departures.txt.gz').distinct()

# Print the number of partitions for each instance
print("Partition count before change: %d" % before)
print("Partition count after change: %d" % departures_df.rdd.getNumPartitions())
```

### Explain

```Python
# Join the flights_df and aiports_df DataFrames
normal_df = flights_df.join(airports_df, \
    flights_df["Destination Airport"] == airports_df["IATA"] )

# Show the query plan
normal_df.explain()
```

## Data pipelines

Una serie de procesos que se realizan para procesar data

1. Input
2. Transformations
3. Output
4. Validation
5. Analisis

```Python
# Import the data to a DataFrame
departures_df = spark.read.csv('2015-departures.csv.gz', header=True)

# Remove any duration of 0
epartures_df = departures_df.filter(departures_df['Actual elapsed time (Minutes)'] > 0)

# Add an ID column
departures_df = departures_df.withColumn('id', F.monotonically_increasing_id())

# Write the file out to JSON format
departures_df.write.json('output.json', mode='overwrite')

```


Remover comentarios de un csv y especificar separadores
```Python
# Import the file to a DataFrame and perform a row count
annotations_df = spark.read.csv('annotations.csv.gz', sep='|')
full_count = annotations_df.count()

# Count the number of rows beginning with '#'
comment_count = annotations_df.where(col('_c0').startswith('#')).count()

# Import the file to a new DataFrame, without commented rows
no_comments_df = spark.read.csv('annotations.csv.gz', sep='|', comment='#')

# Count the new DataFrame and verify the difference is as expected
no_comments_count = no_comments_df.count()
print("Full count: %d\nComment count: %d\nRemaining count: %d" % (full_count, comment_count, no_comments_count))
```

Other transformations
```Python
# Split _c0 on the tab character and store the list in a variable
tmp_fields = F.split(annotations_df['_c0'], '\t')
annotations_df.show()

# Create the colcount column on the DataFrame
annotations_df = annotations_df.withColumn('colcount', F.size(tmp_fields))

# Remove any rows containing fewer than 5 fields
annotations_df_filtered = annotations_df.filter(~ (annotations_df.colcount > 5))

# Count the number of rows
final_count = annotations_df_filtered.count()
print("Initial count: %d\nFinal count: %d" % (initial_count, final_count))
```

```Python
# Split the content of _c0 on the tab character (aka, '\t')
split_cols = F.split(annotations_df["_c0"], '\t')

# Add the columns folder, filename, width, and height
split_df = annotations_df.withColumn('folder', split_cols.getItem(0))
split_df = split_df.withColumn('filename', split_cols.getItem(1))
split_df = split_df.withColumn('width', split_cols.getItem(2))
split_df = split_df.withColumn('height', split_cols.getItem(3))

# Add split_cols as a column
split_df = split_df.withColumn('split_cols', split_cols)
```

Split columns

```Python
def retriever(cols, colcount):
  # Return a list of dog data
  return cols[4:colcount]

# Define the method as a UDF
udfRetriever = F.udf(retriever, ArrayType(StringType()))

# Create a new column using your UDF
split_df = split_df.withColumn('dog_list', udfRetriever(split_df.split_cols, split_df.colcount))

# Remove the original column, split_cols, and the colcount
split_df = split_df.drop('_c0').drop('split_cols').drop('colcount')
```


Store invalid Rows

```Python
# Determine the row counts for each DataFrame
split_count = split_df.count()
joined_count = joined_df.count()

# Create a DataFrame containing the invalid rows
invalid_df = split_df.join(F.broadcast(joined_df), 'folder', 'left_anti')

# Validate the count of the new DataFrame is as expected
invalid_count = invalid_df.count()
print(" split_df:\t%d\n joined_df:\t%d\n invalid_df: \t%d" % (split_count, joined_count, invalid_count))

# Determine the number of distinct folder rows removed
invalid_folder_count = invalid_df.select('folder').distinct().count()
print("%d distinct invalid folders found" % invalid_folder_count)
```

# Analisis

```Python
# Select the dog details and show 10 untruncated rows
print(joined_df.select('dog_list').show(10, truncate=False))

# Define a schema type for the details in the dog list
DogType = StructType([
	StructField("breed", StringType(), False),
    StructField("start_x", IntegerType(), False),
    StructField("start_y", IntegerType(), False),
    StructField("end_x", IntegerType(), False),
    StructField("end_y", IntegerType(), False)
])
```

Parse types 

```Python
# Create a function to return the number and type of dogs as a tuple
def dogParse(doglist):
  dogs = []
  for dog in doglist:
    (breed, start_x, start_y, end_x, end_y) = dog.split(',')
    dogs.append((breed, int(start_x), int(start_y), int(end_x), int(end_y)))
  return dogs

# Create a UDF
udfDogParse = F.udf(dogParse, ArrayType(DogType))

# Use the UDF to list of dogs and drop the old column
joined_df = joined_df.withColumn('dogs', udfDogParse('dog_list')).drop('dog_list')

# Show the number of dogs in the first 10 rows
joined_df.select(F.size('dogs')).show(10)
```


```Python
# Define a UDF to determine the number of pixels per image
def dogPixelCount(doglist):
  totalpixels = 0
  for dog in doglist:
    totalpixels += (dog[3] - dog[1]) * (dog[4] - dog[2])
  return totalpixels

# Define a UDF for the pixel count
udfDogPixelCount = F.udf(dogPixelCount, IntegerType())
joined_df = joined_df.withColumn('dog_pixels', udfDogPixelCount('dogs'))

# Create a column representing the percentage of pixels
joined_df = joined_df.withColumn('dog_percent', (joined_df.dog_pixels / (joined_df.width * joined_df.height)) * 100)

# Show the first 10 annotations with more than 60% dog
joined_df.where('dog_percent > 60').show(10)
```