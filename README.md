# Map Reduce Server
This is an experiment with erlang and map-reduce algorithms.

## README (Spanish)

Hay dos versiones del sistema, una primera que sigue las indicaciones del
enunciado y una segunda que es un experimento inspirado en la API de Spark.
Para poder utilizar la 1� versi�n se tiene que realizar los siguientes pasos:

```# Compilar:
erlc mapredsrv.erl
erlc mapredwrk.erl
erlc mrs_util.erl
erlc test1.erl
# Ejecutar el ejemplo del enunciado:
erl.exe -s test1 example1
# Ejecutar el segundo ejemplo:
erl.exe -s test1 example2```

El segundo ejemplo es un simple contador de palabras, para esta ocasi�n se ha
escogido la letra de una canci�n.

En la segunda versi�n para poder darle uso primero hay que compilar los
siguientes m�dulos:

```erlc mrs_api.erl
erlc mrs_db.erl
erlc mrs_jlogic.erl
erlc mrs_job.erl
erlc mrs_jtask.erl
erlc mrs_planner.erl
erlc mrs_util.erl
erlc mrs_worker.erl
erlc test2.erl```

Despu�s se puede levantar varios nodos utilizando el nombre corto dentro de
una misma m�quina:

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
para el maestro de las peticiones de map-reduce, tambi�n se utilizaron tablas
ETS en vez de diccionarios, como tambi�n se lanza un proceso por cada elemento
por el cual se va a invocar el map.

La topolog�a empleada consiste en un proceso mapredsrv que recibe peticiones,
tras haber creado N procesos mapredwrk. Cuando se va a iniciar el map, se crea
un proceso que lanza las llamadas a cada worker, pas�ndole la funci�n map.
Despu�s se queda a la espera de recibir los resultados y las notificaciones
de finalizaci�n del map por parte de cada worker. Entonces una vez se ha
finalizado la operaci�n por completo, se env�a al maestro una notificaci�n,
pas�ndole el identificador de la tabla ETS con los resultado.

El maestro recibe la petici�n de final del map e inicia el paso de reduce,
de modo similar se crea otro proceso que lanzar� las llamadas a cada worker,
esperar� el resultado y se lo comunicar� al maestro. Cuando el reduce est�
finalizado se env�a al nodo que nos encarg� el trabajo el resultado que se
ha obtenido del proceso.

En la versi�n experimental se nos da una "API" para construir consultas sobre
una estructura de datos o un fichero del disco duro. Cuando queremos ejecutarla
es enviada al nodo planificador de la red de nodos. Cuando se levanta el nodo
planificador levanta en la red que conoce, que puede configurarse con un
fichero de texto que tenga un listado de los hosts de nuestra red, un proceso
worker por cada nodo que existe en su misma red. Una vez recibe el planificador
una consulta, empieza su planificaci�n paso por paso.

Primero se mira si la operaci�n puede paralelizarla el sistema actual, en caso
de poder hacerlo se trocea en fragmentos peque�os los datos de entrada y se
van enviando a los nodos workers de la red para que ejecuten cada paso de la
consulta. A medida que se van recibiendo en orden o no los datos, se
reconstruye la informaci�n recibida en el momento final de recibir todos los
resultados. Tras ello se mira a ver cu�l es el siguiente paso para realizar
el mismo proceso pero con la operaci�n indicada por el paso concreto. Cuando
no quedan m�s pasos se marca la consulta como finalizada y se devuelve el
resultado al cliente del sistema.

En principio la idea que persegu�a pretend�a que se supervisaran los procesos
que se creaban y controlar si, al fallar o caerse uno, hab�a que reenviar alg�n
fragmento de informaci�n para que se volviera a procesar. Pero por falta de
tiempo al haber intentado acometer una idea quiz�s demasiado ambiciosa, no se
pudo implementar esa tolerancia a fallos. Tambi�n como parte de esa tolerancia
se estaban empleando tablas DETS, para que en caso de caer el planificador,
cuando se volviera a levantar recuperara el �ltimo back-up del estado y pudiera
retomar los trabajos por donde quedaron muertos.

Tambi�n se ha intentado dejar anotado en el c�digo con las etiquetas de la edoc
de erlang lo que hace cada funci�n. Igualmente trat� de poner los specs a cada
funci�n mientras aprend�a a usar typer y dem�s. Pero al final no pude tomarme
el tiempo suficiente como para indicar con exactitud los valores que pod�a
recibir un campo o devolverlos.

Adem�s de todo esto est� en la carpeta edoc un m�dulo que de ejecutarse nos
generar� la documentaci�n de los m�dulos de la carpeta del proyecto. Para
ejecutarlo hay que realizar los siguientes pasos:

```erlc zdoc.erl
erl -noshell -s zdoc run -s init stop```
