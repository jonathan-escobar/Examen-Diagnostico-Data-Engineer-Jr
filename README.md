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


## Documentacion Ejercicio 1 


Función Principal: *encontrar_combinacion(lista, objetivo):*
  * Entrada:

    * *lista* : Lista de números entre los cuales se buscarán combinaciones.
    * *objetivo* : Valor que se desea alcanzar mediante la suma de los elementos en las combinaciones.
  * Salida:
    * *resultados* : Lista de listas que representan todas las combinaciones encontradas.
  * Descripción:
      * Inicializa una lista *resultados* para almacenar las combinaciones encontradas. Define una función auxiliar *buscar_combinacion(actual, restantes)* que realiza la búsqueda recursiva de combinaciones. 
      * Llama a la función auxiliar con una llamada inicial *buscar_combinacion([], lista)*. Devuelve la lista de todas las combinaciones encontradas. 
        
  * Función Auxiliar *buscar_combinacion(actual, restantes):*
  
  * Entrada:
    * *actual* : Lista que representa la combinación actual en construcción.
    * *restantes*: Lista de números que aún no se han utilizado en la combinación actual.
    * *Salida* : La función no devuelve ningún valor explícito; en su lugar, agrega combinaciones válidas a la lista resultados.
  * Descripción: Calcula la suma actual de la combinación. Si la suma es igual al objetivo, agrega la combinación actual a la lista de resultados. Utiliza recursividad para explorar todas las combinaciones posibles con los elementos restantes.

##### Ejemplo de Uso:

*#Lista de números y valor objetivo*

*lista_numeros = [1, 2, 5, 3]*

*valor_objetivo = 6*

*#Llamada a la función principal para encontrar combinaciones*

*resultado_combinaciones = encontrar_combinacion(lista_numeros, valor_objetivo)*

*#Imprimir el resultado*

*print(resultado_combinaciones)*


*Resultado Esperado:*
*[[1, 2, 3], [1, 5]]*


##### Paso a Paso del Ejemplo de Uso:
  
* *Llamada Inicial* :
    * Se llama a *encontrar_combinacion* con la lista [1, 2, 5, 3] y el valor objetivo 6.

* *Inicialización* : 
    * *resultados* se inicializa como una lista vacía.

* *Llamada a buscar_combinacion Inicial:* 
    * Se llama a *buscar_combinacion* con los argumentos iniciales ([], [1, 2, 5, 3]).

* *Exploración Recursiva* :
    * Se exploran todas las combinaciones posibles mediante la función *buscar_combinacion*. 
    * Las combinaciones [1, 2, 3] y [1, 5] se encuentran y se agregan a resultados.

* *Resultado Impreso*:
    * El resultado final, que son todas las combinaciones que suman 6, se imprime como [[1, 2, 3], [1, 5]].





