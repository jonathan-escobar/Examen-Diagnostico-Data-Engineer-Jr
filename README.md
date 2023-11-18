%md
# Examen Diagnostico Data Engineer Jr
**cuentas con 24 horas para resolver el ejercicio**

## Instrucciones

1. Debes crear un repositorio de github en dónde entregarás tu notebook
2. Crear una rama que por nombre lleve tus iniciales.
3. Es importante que el primer commit se haga con tres archivos:
    * _README.md_ contendrá estas instrucciones
    * _solution.ipynb_ contendrá la exportación de este notebook como ipynb
    * _players_21.csv_ será el archivo de datos csv tomado desde: https://raw.githubusercontent.com/dagarciam/diagnostico_pyspark/solution/resources/data/players_21.csv
4. El ejercicio debe ser resuelto en un notebook de Databricks Community
    * Aquí puedes encontrar como crear tu cuenta gratuita: https://docs.databricks.com/en/getting-started/community-edition.html
6. Agregué una sección a este README.md 
    * Documente el algoritmo implementado para resolver el **Ejercicio 1** utilizando un ejemplo y mostrando paso a paso como se encuentra la solución.
    * Con las instrucciones de uso de la función `run_process` y un ejemplo de uso.

_**Haz que tu repositorio sea privado** y brinda acceso de lectrua a tu repositorio al usuario **(indicado en el correo)**_

## ¿Qué evaluaremos?

* El Ejercicio 1 y 2 debe realizarse sin librerias
* Resuelva el Ejercicio 3 con el API de SparkSQL el ejercicio planteado.
* El uso de sentencias SQL queda estrictamente prohibido.
* El uso de cadenas en las clases que implementan la lógica de solución están muy mal vistos por nuestra area de QA, sea
  cuidadoso.
* Modularice sú código lo suficiente de tal forma que cada función haga una sola cosa.

## Ejercicio 1

1. Desarrolla un método que dada una lista de números enteros encuentre los valores que sumados den como resultado un valor objetivo.
  * El método recibirá los siguientes parámetros:
    1. Lista con valores enteros
    2. Valor objetivo
  * El método deberá de retornar una lisla con los valores obtenidos.
  * Para obtener la combinación de números a retornar sólo será posible utilizar una vez cada elemento de la lista enviada como parámetro (la lista puede contener valores repetidos)
##### Ejemplo 
```
my_list = [1, 2, 5, 3]
target = 6
result = my_function(my_list, target)
```
##### Resultado 
`result = [[1, 5], [1, 2, 3]]`
Porque ambas combinaciones suman 6.

## Ejercicio 2

Desarrolla un programa que dadas dos listas A y B genere una lista C que contendrá en la i-ésima posición el conteo de elementos de A que son menores a la i-ésima posición de B.
 
##### Ejemplo
```
A = [1, 6, 3, 8, 1, 3]
B = [6, 2, 1, 9]

C = my_function(A,B)
```
##### Resultado

```
C = [4, 2, 0, 6]
```

##### Explicación:
* _La primer posición de C es 4 ya que sólo existen 4 números de la lista A menores al valor 6 de la primer posición de B_
* _La segunda posición de C es 2 ya que sólo existen 2 números de la lista A menores al valor 2 de la segunda posición de B_
* _La tercer posición de C es 0 ya que no existen números de la lista A menores al valor 1 de la tercer posición de B_
* _La cuarta posición de C es 6 ya que existen 6 números de la lista A menores al valor 9 de la cuarta posición de B_

## Ejercicio 3
1. La tabla de salida debe contener las siguientes columnas:
   `short_name, long_name, age, height_cm, weight_kg, nationality, club_name, overall, potential, team_position`
2. Agregar una columna `player_cat` que responderá a la siguiente regla (rank over Window particionada por `nationality` y `team_position`
   y ordenada por `overall`):
    * **A** si el jugador es de los mejores 3 jugadores en su posición de su país.
    * **B** si el jugador es de los mejores 5 jugadores en su posición de su país.
    * **C** si el jugador es de los mejores 10 jugadores en su posición de su país.
    * **D** para el resto de jugadores.

   ***tip** para resolver este ejercicio, este blog puede ayudar: https://sparkbyexamples.com/pyspark/pyspark-window-functions
3. Agregaremos una columna `potential_vs_overall` con la siguiente regla:
    * Columna `potential` dividida por la columna `overall`
4. Filtraremos de acuerdo a las columnas `player_cat` y `potential_vs_overall` con las siguientes condiciones:
    * Si `player_cat` esta en los siguientes valores: **A**, **B**
    * Si `player_cat` es **C** y `potential_vs_overall` es superior a **1.15**
    * Si `player_cat` es **D** y `potential_vs_overall` es superior a **1.25**
5. Por favor escriba la tabla resultante de los pasos anteriores particionada por la columna `nationality`, la salida
   debe estar escrita en formato **parquet** y debe usarse el método `coalese(1)`
   para obtener solo un archivo por partición.
6. Agregar el método `run_process` que reciba cuatro parametros 
    * str con el path de donde se van a leer los datos origen
    * str con el path donde se van a escribir los datos de salida
    * int que de ser 1 realizará todos los pasos únicamente para los jugadores menores de 23 años y en caso de ser 0 lo hará con todos los jugadores del dataset
    * `nationality_filter` que de ser `None` no realizará cambios a la salida y de ser cualquier otro valor filtrará la salida con la siguiente condición `col('nationality') == nationality_filter` antes de escribirla.

¡Buena suerte!

## Ejercicio Numero 1 Documentacion 

"""
# Encontrar Combinaciones que Sumen un Objetivo

## Descripción
Esta función implementa un algoritmo para encontrar todas las combinaciones de números en una lista que suman un objetivo dado. Utiliza una estrategia de búsqueda exhaustiva mediante recursión.

## Función Principal
```
def encontrar_combinacion(lista, objetivo):
    resultados = []  # Lista para almacenar las combinaciones encontradas
    
    # Función auxiliar para la búsqueda de combinaciones
    def buscar_combinacion(actual, restantes):
        suma_actual = sum(actual)
        
        # Verificar si la suma actual es igual al objetivo
        if suma_actual == objetivo:
            resultados.append(actual)
            return
        
        # Iterar sobre los elementos restantes
        for i, num in enumerate(restantes):
            # Llamada recursiva para explorar combinaciones con el elemento actual
            buscar_combinacion(actual + [num], restantes[i + 1:])
    
    # Llamada inicial a la función auxiliar
    buscar_combinacion([], lista)
    
    return resultados 
```

# Definir la lista de números y el valor objetivo
lista_numeros = [1, 2, 5, 3]
valor_objetivo = 6

# Llamada a la función principal para encontrar combinaciones
resultado_combinaciones = encontrar_combinacion(lista_numeros, valor_objetivo)

# Imprimir el resultado
print(resultado_combinaciones)

"""
## Pasos del Algoritmo

1. **Inicialización:** Se inicia con una lista vacía (`actual`) que representa la combinación actual y la lista completa de números (`restantes`).
2. **Verificación:** Se verifica si la suma de la combinación actual es igual al objetivo. Si es así, se añade a los resultados.
3. **Iteración Recursiva:** Se itera sobre los números restantes, y para cada número, se realiza una llamada recursiva con la combinación actual más ese número y la lista de números restantes desde la posición siguiente.

## Ejemplo con Explicación

- **Lista de Números:** `[1, 2, 5, 3]`
- **Objetivo:** `6`

1. **Primera llamada:** `buscar_combinacion([], [1, 2, 5, 3])`
    - Se evalúa con `actual=[]` y `restantes=[1, 2, 5, 3]`.

2. **Segunda llamada:** `buscar_combinacion([1], [2, 5, 3])`
    - Se evalúa con `actual=[1]` y `restantes=[2, 5, 3]`.

3. **Tercera llamada:** `buscar_combinacion([1, 2], [5, 3])`
    - Se evalúa con `actual=[1, 2]` y `restantes=[5, 3]`.
    - Se encuentra la combinación `[1, 2, 3]` que suma 6, se añade a los resultados.

4. **Cuarta llamada:** `buscar_combinacion([1, 5], [3])`
    - Se evalúa con `actual=[1, 5]` y `restantes=[3]`.
    - Se encuentra la combinación `[1, 5]` que suma 6, se añade a los resultados.

5. **Quinta llamada:** `buscar_combinacion([1, 3], [])`
    - Se evalúa con `actual=[1, 3]` y `restantes=[]`.
    - No se encuentra ninguna combinación adicional.

Resultado Final: `[[1, 2, 3], [1, 5]]`
"""



## Ejercicio Numero 3 funcion *run_procces* 

# ProcesoDatos: Clase para el Procesamiento de Datos con PySpark

La clase `ProcesoDatos` proporciona métodos para cargar, transformar y guardar datos utilizando PySpark.

## Constructor
```python
def __init__(self):
    """
    Inicializa la sesión de Spark.

    Parámetros:
    - Ninguno

    Retorna:
    - None
    """
    # Inicializar la sesión de Spark
    self.spark = SparkSession.builder.appName("ProcesoDatos").getOrCreate()

  def cargar_datos(self, path_origen):
    """
    Carga datos desde un archivo CSV.

    Parámetros:
    - `path_origen` (str): Ruta del archivo CSV.

    Retorna:
    - DataFrame: DataFrame de PySpark con los datos cargados.
    """
    # Cargar datos desde el origen
    df = self.spark.read.format("csv").option("header", "true").load(path_origen)
    return df


  def agregar_columnas(self, df, filtro_edad=0, nationality_filter=None):
    """
    Agrega columnas al DataFrame y aplica filtros opcionales.

    Parámetros:
    - `df` (DataFrame): DataFrame de entrada.
    - `filtro_edad` (int): Filtro opcional por edad. Valor por defecto: 0.
    - `nationality_filter` (str): Filtro opcional por nacionalidad. Valor por defecto: None.

    Retorna:
    - DataFrame: DataFrame con las columnas agregadas y filtros aplicados.
    """
    # Aplicar filtro de edad si es necesario
    if filtro_edad == 1:
        df = df.filter(F.col("age") < 23)
    
    # Agregar columna 'potential_vs_overall'
    df = df.withColumn("potential_vs_overall", F.col("potential") / F.col("overall"))

    # Agregar columna 'player_cat' con reglas de clasificación
    window_spec = Window.partitionBy("nationality", "team_position").orderBy(F.col("overall").desc())
    df_ranked = df.withColumn("position_rank", F.rank().over(window_spec))
    df_result = df_ranked.withColumn(
        "player_cat",
        F.when(F.col("position_rank") <= 3, "A")
        .when(F.col("position_rank") <= 5, "B")
        .when(F.col("position_rank") <= 10, "C")
        .otherwise("D")
    )

    # Aplicar filtro de nacionalidad si es necesario
    if nationality_filter is not None:
        df_result = df_result.filter(F.col('nationality') == nationality_filter)

    return df_result

  def guardar_datos(self, df_result, path_destino):
    """
    Guarda el DataFrame resultante en formato Parquet.

    Parámetros:
    - `df_result` (DataFrame): DataFrame resultante.
    - `path_destino` (str): Ruta de destino para guardar los datos en formato Parquet.

    Retorna:
    - None
    """
    # Escribir datos en formato Parquet
    df_result.coalesce(1).write.partitionBy("nationality").parquet(path_destino, mode="overwrite")

  def run_process(self, path_origen, path_destino, filtro_edad=0, nationality_filter=None):
    """
    Ejecuta el proceso completo: carga datos, agrega columnas, aplica filtros y guarda el resultado.

    Parámetros:
    - `path_origen` (str): Ruta del archivo CSV de origen.
    - `path_destino` (str): Ruta de destino para guardar los datos en formato Parquet.
    - `filtro_edad` (int): Filtro opcional por edad. Valor por defecto: 0.
    - `nationality_filter` (str): Filtro opcional por nacionalidad. Valor por defecto: None.

    Retorna:
    - None
    """
    # Cargar datos desde el origen
    df = self.cargar_datos(path_origen)

    # Agregar columnas y aplicar filtros
    df_result = self.agregar_columnas(df, filtro_edad, nationality_filter)

    # Guardar datos en el destino
    self.guardar_datos(df_result, path_destino)

    # Mostrar el DataFrame resultante (se asume que `display` es una función disponible)
    display(df_result)


  # Ejemplo de uso
proceso = ProcesoDatos()
proceso.run_process("/FileStore/tables/players_21_local.csv", "/FileStore/tables/output", filtro_edad=1, nationality_filter="Spain")
