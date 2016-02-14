# Map Reduce Server
This is an experiment with erlang and map-reduce algorithms.

## README (Spanish)

Hay dos versiones del sistema, una primera que sigue las indicaciones del
enunciado y una segunda que es un experimento inspirado en la API de Spark.
Para poder utilizar la 1ª versión se tiene que realizar los siguientes pasos:

```# Compilar:
erlc mapredsrv.erl
erlc mapredwrk.erl
erlc mrs_util.erl
erlc test1.erl
# Ejecutar el ejemplo del enunciado:
erl.exe -s test1 example1
# Ejecutar el segundo ejemplo:
erl.exe -s test1 example2```

El segundo ejemplo es un simple contador de palabras, para esta ocasión se ha
escogido la letra de una canción.

En la segunda versión para poder darle uso primero hay que compilar los
siguientes módulos:

```erlc mrs_api.erl
erlc mrs_db.erl
erlc mrs_jlogic.erl
erlc mrs_job.erl
erlc mrs_jtask.erl
erlc mrs_planner.erl
erlc mrs_util.erl
erlc mrs_worker.erl
erlc test2.erl```

Después se puede levantar varios nodos utilizando el nombre corto dentro de
una misma máquina:

```gnome-terminal -e 'erl.exe -sname N0'
gnome-terminal -e 'erl.exe -sname N1'
gnome-terminal -e 'erl.exe -sname N2'
gnome-terminal -e 'erl.exe -sname N3'```

Desde uno de los nodos se puede ejecutar algunos ejemplos:

```test2:start().    % Para arrancar el planificador.
test2:example().  % Para ejecutar el ejemplo del enunciado.
test2:example1(). % Para ejecutar un contador de palabras.
test2:example2(). % Para ejecutar un ejemplo sobre un log de accesos.
test2:example3(). % Para ejecutar un ejemplo sobre un log de temperaturas.
test2:example4(). % Para ejecutar un ejemplo sobre un log de palabras.```

En la propuesta por el enunciado se ha utilizado el comportamiento gen_server
para el maestro de las peticiones de map-reduce, también se utilizaron tablas
ETS en vez de diccionarios, como también se lanza un proceso por cada elemento
por el cual se va a invocar el map.

La topología empleada consiste en un proceso mapredsrv que recibe peticiones,
tras haber creado N procesos mapredwrk. Cuando se va a iniciar el map, se crea
un proceso que lanza las llamadas a cada worker, pasándole la función map.
Después se queda a la espera de recibir los resultados y las notificaciones
de finalización del map por parte de cada worker. Entonces una vez se ha
finalizado la operación por completo, se envía al maestro una notificación,
pasándole el identificador de la tabla ETS con los resultado.

El maestro recibe la petición de final del map e inicia el paso de reduce,
de modo similar se crea otro proceso que lanzará las llamadas a cada worker,
esperará el resultado y se lo comunicará al maestro. Cuando el reduce está
finalizado se envía al nodo que nos encargó el trabajo el resultado que se
ha obtenido del proceso.

En la versión experimental se nos da una "API" para construir consultas sobre
una estructura de datos o un fichero del disco duro. Cuando queremos ejecutarla
es enviada al nodo planificador de la red de nodos. Cuando se levanta el nodo
planificador levanta en la red que conoce, que puede configurarse con un
fichero de texto que tenga un listado de los hosts de nuestra red, un proceso
worker por cada nodo que existe en su misma red. Una vez recibe el planificador
una consulta, empieza su planificación paso por paso.

Primero se mira si la operación puede paralelizarla el sistema actual, en caso
de poder hacerlo se trocea en fragmentos pequeños los datos de entrada y se
van enviando a los nodos workers de la red para que ejecuten cada paso de la
consulta. A medida que se van recibiendo en orden o no los datos, se
reconstruye la información recibida en el momento final de recibir todos los
resultados. Tras ello se mira a ver cuál es el siguiente paso para realizar
el mismo proceso pero con la operación indicada por el paso concreto. Cuando
no quedan más pasos se marca la consulta como finalizada y se devuelve el
resultado al cliente del sistema.

En principio la idea que perseguía pretendía que se supervisaran los procesos
que se creaban y controlar si, al fallar o caerse uno, había que reenviar algún
fragmento de información para que se volviera a procesar. Pero por falta de
tiempo al haber intentado acometer una idea quizás demasiado ambiciosa, no se
pudo implementar esa tolerancia a fallos. También como parte de esa tolerancia
se estaban empleando tablas DETS, para que en caso de caer el planificador,
cuando se volviera a levantar recuperara el último back-up del estado y pudiera
retomar los trabajos por donde quedaron muertos.

También se ha intentado dejar anotado en el código con las etiquetas de la edoc
de erlang lo que hace cada función. Igualmente traté de poner los specs a cada
función mientras aprendía a usar typer y demás. Pero al final no pude tomarme
el tiempo suficiente como para indicar con exactitud los valores que podía
recibir un campo o devolverlos.

Además de todo esto está en la carpeta edoc un módulo que de ejecutarse nos
generará la documentación de los módulos de la carpeta del proyecto. Para
ejecutarlo hay que realizar los siguientes pasos:

```erlc zdoc.erl
erl -noshell -s zdoc run -s init stop```
