# Evaluación de rendimiento y fiabilidad

Este evaluador compara dos variantes del mismo flujo de entrega y devolución:

- `postgres_pool`: peticiones que terminan la operación antes de responder,
  utilizando conexiones asíncronas reutilizadas e idempotencia transaccional.
- `event_driven`: las escrituras se aceptan como comandos, RabbitMQ las entrega
  al worker y el cliente consulta solo los dos resultados necesarios para avanzar
  al siguiente paso. La finalización de la devolución se comprueba después de la
  carga en PostgreSQL. API y worker usan `postgres_pool`, con el mismo dominio y
  casos de uso.

Las dos variantes utilizan `async/await`. Aquí «directo» significa que la
operación termina dentro de la petición HTTP; no implica código Python bloqueante.

Se ejecuta desde la terminal y guarda archivos locales. No necesita un servidor
Grafana, una cuenta ni un servicio permanente de monitorización. k6 es el programa
que genera las peticiones HTTP; lo ejecutamos en un contenedor temporal.

## Cómo encajan las piezas

```text
python3 benchmarks/run.py
    │
    ├── Arranca un entorno de evaluación con Docker Compose
    │       ├── FastAPI con persistencia PostgreSQL mediante pool
    │       ├── PostgreSQL con una base de datos dedicada
    │       └── RabbitMQ y worker, solo en event_driven
    │
    ├── Prepara datos y comprueba conflictos y solicitudes repetidas
    ├── Ejecuta k6: crear entrega → entregar → solicitar devolución
    ├── En eventos, deja terminar el trabajo pendiente con un límite de espera
    ├── Audita los flujos completos y calcula latencias y rendimiento
    ├── Recoge consumo de recursos y comprueba los datos guardados
    ├── Repite con la siguiente variante
    └── Guarda los resultados y elimina los contenedores de evaluación
```

`run.py` coordina el experimento desde tu máquina usando la biblioteca estándar
de Python. k6 registra las solicitudes y un recibo por cada flujo iniciado. Los
auxiliares de preparación y verificación se ejecutan dentro del contenedor de la
API, aprovechando psycopg. El resultado principal es el flujo completo auditado,
no la cantidad de respuestas HTTP ni de comandos aceptados.

El entorno usa `benchmarks/compose.yml` con un nombre de proyecto único
`return-flow-bench-<id>`. Su base de datos es `return_flow_benchmark`: no utiliza
los puertos, los datos ni los volúmenes del entorno de desarrollo. Cada ejecución
crea un volumen propio para PostgreSQL y lo elimina antes de la siguiente; al
terminar el experimento también se elimina. Los resultados permanecen en el disco
de tu máquina.

El presupuesto predeterminado de **aplicación** es de 2 CPU y 1 GiB de memoria:

| Variante | API | Worker | PostgreSQL | RabbitMQ | k6 |
|---|---|---|---|---|---|
| `postgres_pool` | 2 CPU, 1 GiB, 2 procesos | — | 1 CPU, 512 MiB | — | 1 CPU, 512 MiB |
| `event_driven` | 1 CPU, 512 MiB, 1 proceso | 1 CPU, 512 MiB, 1 proceso | 1 CPU, 512 MiB | 1 CPU, 512 MiB | 1 CPU, 512 MiB |

PostgreSQL y el generador quedan fuera del presupuesto de aplicación. RabbitMQ
añade infraestructura a `event_driven`: los servicios de esa variante suman
4 CPU frente a 3 CPU en la directa, sin contar k6. La API se ejecuta sin recarga
automática ni registro de cada petición HTTP.

`--app-cpus` cambia el presupuesto de aplicación; en eventos se reparte por igual
entre API y worker. `--api-workers` configura los procesos de la variante
directa; eventos mantiene un proceso de API y un worker. El worker procesa hasta
ocho comandos simultáneos por defecto; `--worker-concurrency` ajusta ese límite y
el prefetch al mismo valor. Las tareas siguen compartiendo la CPU asignada al
worker. El publicador
permite hasta 64 publicaciones pendientes de confirmación por proceso de API;
`--publish-concurrency` ajusta ese límite. Cada POST sigue esperando su propia
confirmación de RabbitMQ antes de responder `202`, usando `wait=False` para evitar
la espera intermedia de escritura local bajo el bloqueo del canal. El registro
previo del comando en PostgreSQL y la idempotencia transaccional se mantienen.

Los pools de ambas API usan autocommit para lecturas aisladas; las escrituras de
comandos conservan sus transacciones explícitas y el pool del worker mantiene su
configuración transaccional. El healthcheck de la API se ejecuta cada segundo
durante el arranque y cada 30 segundos después; la imagen precalcula el bytecode
de sus importaciones para reducir el coste dentro de la cuota de CPU de la API.

## Ejecutar

Requiere Python 3.10 o posterior y acceso a Docker con Docker Compose. No necesitas
instalar k6 ni dependencias Python adicionales en tu máquina. La primera ejecución
descarga las imágenes y construye la API.

Desde la raíz del proyecto, empieza por una comprobación corta:

```bash
python3 benchmarks/run.py --smoke
```

Esta prueba ejecuta el flujo completo en las dos variantes, a 2 flujos por
segundo, con 1 segundo de calentamiento, 3 de medición y una repetición. Utiliza
10 usuarios virtuales preparados y como máximo. Comprueba que el evaluador
funciona; es demasiado corta para concluir qué adaptador rinde mejor. Son dos
mediciones.

Después, ejecuta la comparación predeterminada del flujo completo:

```bash
python3 benchmarks/run.py
```

Evalúa `flow` con las dos variantes, a 10, 50 y 400 flujos por segundo, con
5 segundos de calentamiento, 30 de medición y tres repeticiones. Son 18
mediciones previstas. También se miden las configuraciones que incumplan los
umbrales durante el calentamiento. Abre el `report.md` de la carpeta de resultados
que imprime el script para comparar cada repetición.

La selección explícita equivalente es:

```bash
python3 benchmarks/run.py \
  --adapters postgres_pool event_driven \
  --app-cpus 2 --api-workers 2 \
  --rates 10 50 400 \
  --warmup 5 \
  --duration 30 \
  --repetitions 3
```

Para una primera comparación más corta con 2 CPU de aplicación, esta matriz hace
12 mediciones de 20 segundos, además de los calentamientos y verificaciones:

```bash
python3 benchmarks/run.py \
  --adapters postgres_pool event_driven \
  --app-cpus 2 --api-workers 2 \
  --scenarios flow --rates 10 50 400 \
  --warmup 3 --duration 20 --repetitions 2
```

El pool mantiene una conexión como mínimo **por proceso**. Con el máximo
predeterminado de 10 y dos procesos de API, `postgres_pool` puede abrir hasta
20 conexiones. En eventos hay un pool en la API y otro en el worker, también
con máximo 10 cada uno; el worker ejecuta hasta ocho comandos simultáneos por
defecto. Aumentar su concurrencia por encima del pool puede añadir esperas por
conexiones, por lo que ambos límites deben ajustarse conjuntamente.
Las dos variantes utilizan el mismo almacenamiento y ejecutor de comandos:
registran la clave, bloquean el comando y confirman su resultado junto con la
escritura de negocio en una transacción. La variante directa ejecuta dentro de la
petición; la otra publica en RabbitMQ y ejecuta en el worker. Se compara el coste
del transporte y seguimiento del trabajo con la misma garantía de idempotencia.
La persistencia no tiene un selector de adaptadores.

Para elegir otros ritmos y prolongar las mediciones:

```bash
python3 benchmarks/run.py \
  --adapters postgres_pool event_driven \
  --scenarios flow \
  --rates 10 50 100 \
  --warmup 30 \
  --duration 120 \
  --repetitions 3
```

Cada combinación de adaptador, ritmo y repetición es una ejecución independiente.
El ejemplo anterior realiza 18 mediciones, además de sus calentamientos: reserva
tiempo suficiente. Solo se admite `flow`; `read` y `create` ya no son escenarios
de este evaluador.

| Argumento | Valor por defecto | Qué controla |
|---|---|---|
| `--adapters` | `postgres_pool event_driven` | Variantes que se comparan. |
| `--app-cpus` | `2` | CPU de aplicación: todo para API directa; mitad API y mitad worker en eventos. |
| `--api-workers` | `2` | Procesos de API directa; eventos utiliza uno. |
| `--scenarios` | `flow` | Único escenario admitido: flujo completo de entrega y devolución. |
| `--rates` | `10 50 400` | Flujos que se intenta iniciar por segundo, no peticiones HTTP. |
| `--duration` | `30` | Segundos de medición por ejecución. |
| `--warmup` | `5` | Segundos de calentamiento antes de medir. |
| `--repetitions` | `3` | Repeticiones de cada combinación. |
| `--seed-size` | `1000` | Entregas iniciales del conjunto de datos. |
| `--pool-size` | `10` | Máximo de conexiones del adaptador con pool, por proceso. |
| `--pool-timeout` | `5` | Segundos máximos para esperar una conexión del pool. |
| `--preallocated-vus` | `200` | Usuarios virtuales preparados al iniciar k6. |
| `--max-vus` | `200` | Límite de usuarios virtuales de k6. |
| `--request-timeout` | `5` | Tiempo máximo en segundos por petición HTTP. |
| `--command-timeout` | `10` | Espera máxima en segundos para cada uno de los dos comandos de los que depende el siguiente paso. |
| `--poll-interval` | `0.1` | Espera inicial en segundos después de observar un comando pendiente. |
| `--poll-max-interval` | `1` | Máximo de la espera exponencial entre consultas de dependencias. |
| `--drain-timeout` | `30` | Segundos adicionales para terminar comandos pendientes tras finalizar k6. |
| `--publish-concurrency` | `64` | Máximo de publicaciones pendientes de confirmación por proceso de API. |
| `--worker-concurrency` | `8` | Comandos simultáneos del worker y prefetch; se guarda en el manifiesto y en cada configuración. |
| `--no-build` | Desactivado | Reutiliza la imagen de la API ya construida. |

Un usuario virtual ejecuta un flujo cada vez. k6 utiliza tantos como necesita
para mantener el ritmo solicitado, hasta el límite indicado. Si las respuestas
tardan más, hacen falta más usuarios concurrentes para mantener ese ritmo.
A 400 flujos/s, 200 usuarios solo bastan si cada flujo ocupa, en promedio, como
máximo 0,5 segundos. El polling también mantiene ocupado al usuario virtual.
Comprueba los descartes y la CPU de k6 antes de atribuir una limitación al servidor.

## Qué se prueba

El único recorrido medido es crear una entrega, marcarla como entregada y
solicitar su devolución. Cada flujo utiliza entidades nuevas y sus resultados se
contrastan después con las filas persistidas y sus relaciones.

| Variante | Solicitudes durante un flujo sin esperas adicionales |
|---|---|
| `postgres_pool` | Tres POST: crear entrega, entregar y solicitar devolución. No hay GET de verificación. |
| `event_driven` | Tres POST de comandos y dos GET de estado como mínimo, para obtener la entrega creada y confirmar que se ha entregado antes de solicitar la devolución. |

La primera consulta de cada dependencia es inmediata. Si sigue pendiente, se
esperan 100 ms, 200 ms, 400 ms, 800 ms y después hasta 1 s entre consultas, con los
valores predeterminados. La espera y el timeout de cada petición se acotan al
plazo de `--command-timeout`. No se consulta el tercer comando ni se hace un GET
de la devolución durante la carga: se verifican al terminar. Así se reduce la
interferencia de observación, aunque las dos dependencias todavía añaden
consultas HTTP/SQL y espera real al flujo.

Una respuesta `202` solo confirma aceptación. En `summary.json`,
`workflows_submitted` indica que la última operación recibió la respuesta
esperada; en eventos, `workflows_completed` permanece a cero porque k6 no observa
la finalización del último comando. Los completados y sus percentiles finales se
calculan en la auditoría y aparecen en `verification.json`, `result.json`, el CSV
y el informe. Las latencias de los POST asíncronos siguen disponibles como
medidas de aceptación, separadas del resultado E2E.

Antes de generar carga se comprueba, una vez por variante, el mismo contrato:

- Repetir concurrentemente la misma clave crea una sola entrega o devolución y
  conserva su resultado, también al reintentar después de completarse.
- Entregar una entrega ya entregada devuelve el mismo estado, incluso con otra
  clave: ambas rutas ejecutan `ensure_delivery_delivered`.
- Claves distintas de devolución crean devoluciones distintas para la misma
  entrega; la idempotencia no impone una devolución única por entrega.
- Reutilizar una clave con otro contenido u operación devuelve `409`.
- Solicitar una devolución para una entrega pendiente o inexistente no crea una
  devolución: el comando guarda `failed` y repetir la clave conserva ese fallo.
  La directa responde `409` o `404`; en eventos el fallo se consulta después de
  recibir `202`.

El preflight consulta los resultados y las filas persistidas en ambas variantes,
fuera de la carga medida, y guarda el resultado en `reliability-*.json`. Durante
la carga, los tres POST de ambas variantes llevan `Idempotency-Key: <UUID>`;
la directa sigue realizando únicamente tres POST, sin consultas adicionales.


## Metodología

Cada ejecución parte de datos preparados de nuevo. Se prepara el conjunto inicial
antes del calentamiento y se restaura antes de la medición, para que los datos
escritos durante el calentamiento no cambien el punto de partida. Se alterna el
orden de los adaptadores entre repeticiones para reducir el efecto de ejecutarlos
siempre en el mismo orden.

En eventos, una vez que k6 termina, se detiene primero la API de forma ordenada
para que ninguna solicitud todavía en curso registre o publique comandos después
de comprobar el trabajo pendiente. El worker continúa ejecutándose. Un auxiliar
consulta el número total de comandos pendientes en PostgreSQL una vez por segundo,
con una última comprobación al agotar el plazo, durante un máximo de 30 segundos
por defecto (`--drain-timeout`). Esta espera adicional solo ocurre después de la
carga, no hace polling HTTP por comando y queda registrada en `command-drain.json`.
Después se detiene el worker, se comprueba el cierre de conexiones y se auditan
los datos y las colas. Agotar el plazo hace fallar la fase aunque algún comando
alcance a terminar durante el cierre posterior.

El calentamiento se registra y audita por separado. Incumplir sus umbrales no
impide medir: precisamente queremos observar los errores cuando una configuración
se satura. Antes de restaurar los datos se comprueba que puede hacerse sin que
queden operaciones anteriores pendientes:

- En la variante directa, si todos los flujos iniciados terminan con respuestas válidas, se conserva el
  proceso de la API y su pool caliente. Esto también se aplica cuando k6 haya
  descartado operaciones que no llegó a iniciar.
- En la variante directa, si hay errores, flujos incompletos o escrituras de resultado desconocido, se
  detiene la API y se espera a que desaparezcan sus conexiones a PostgreSQL.
  Entonces se audita el calentamiento, se reinicia la API y se preparan los datos
  de la medición. Se registra `warmup_recovery: "api_restarted"`: esta medición
  puede empezar con el proceso y el pool fríos, lo que hay que considerar al
  interpretar sus latencias.
- En eventos siempre se detienen **API y worker**, se espera el cierre de sus
  conexiones y se auditan comandos, datos y colas. Antes de preparar la siguiente
  fase se purgan la cola y su DLQ, y se vacían también los registros de comandos.
  Los datos y el trabajo pendiente del calentamiento quedan en sus artefactos
  antes del reset. Se registra `warmup_recovery: "api_and_worker_restarted"`:
  los procesos y pools de eventos se reinician antes de la medición, incluso si
  el calentamiento fue correcto. Esta diferencia limita la comparación de
  latencias de arranque con las variantes que conservan el pool caliente.

En la variante directa, si la medición termina con errores o resultados
incompletos, también se detiene la API y se espera a que desaparezcan sus
conexiones antes de auditar los datos. En eventos se aplica siempre la secuencia
de cierre, espera acotada y auditoría descrita arriba. Si no se puede confirmar
que han terminado las operaciones en la base de datos, se aborta el experimento
en lugar de reinicializarla.

k6 intenta iniciar operaciones a un ritmo fijo. Así, cuando la API se ralentiza,
el cliente no reduce automáticamente la carga por esperar las respuestas. Si no
puede iniciar alguna operación, se registra como una iteración descartada. Una
ejecución con descartes no demuestra que la API haya sostenido el ritmo solicitado.

Se conservan por ejecución:

- Flujos completos verificados por segundo (*goodput*) y latencias E2E p50, p95
  y p99, calculados después de la carga. Solo los flujos terminados con datos y
  relaciones correctos aportan una muestra de latencia.
- Latencias por endpoint y recuentos HTTP como diagnóstico; no sustituyen las
  métricas de flujo completo.
- Peticiones HTTP correctas y fallidas, con recuentos y porcentajes, y el desglose
  de fallos por respuestas `4xx`, `5xx`, errores de transporte y respuestas
  inesperadas. Los timeouts son parte de los errores de transporte.
- Flujos iniciados, completados, fallidos, pendientes, con comandos ausentes o
  incompletos; e iteraciones descartadas antes de iniciarse. Los resultados del
  calentamiento aparecen por separado.
- Muestras de CPU y memoria de API, PostgreSQL y k6, además de worker y RabbitMQ
  en eventos, mediante `docker stats`. El muestreo espera al arranque de k6 y deja
  dos segundos entre consultas; cada consulta también necesita tiempo. Solo
  toma muestras mientras existe el contenedor de k6; no mide el drenaje posterior.
- Comprobación de entidades, estados y relaciones contra los recibos de flujo y
  los resultados persistidos de los tres comandos en ambas variantes.
- En ambas: comandos intentados, aceptados, completados, fallidos, pendientes o
  ausentes. Solo en eventos: timeouts de observación de dependencias y mensajes
  restantes en la cola de trabajo y en la DLQ. Las colas se inspeccionan con API y worker detenidos,
  después de auditar la fase y antes de purgar.

Cada intento de flujo escribe un recibo con `flow_id`, `started_at_ms` y
`finished_at_ms`, incluso si falla la primera petición. Incluye los identificadores
de las entidades confirmadas y todos los `command_id` intentados y los
que recibieron aceptación. Un error o timeout del POST no elimina ese identificador:
la auditoría puede localizar su resultado después. Antes del POST de devolución
también se registra `return_attempted`, para poder atribuir el resultado directo
si se pierde esa respuesta. Los recibos añaden trabajo al generador, pero no
consultas al sistema medido.

La latencia E2E utiliza el mismo límite en las dos variantes: empieza en el
cliente justo antes de la primera solicitud y termina con el **commit de la
transacción que crea la devolución**. La auditoría obtiene ese instante mediante
`pg_xact_commit_timestamp(returns.xmin)` y exige que la entrega y la devolución
coincidan con el flujo. En ambas exige además los tres comandos completados y
sus resultados correctos. La latencia principal no incluye esperar a observar el
resultado final ni la respuesta HTTP posterior al commit. La latencia hasta la
respuesta directa de k6 queda disponible como diagnóstico en `summary.json`.

Solo el PostgreSQL del entorno de benchmark activa `track_commit_timestamp=on`,
en **todas las variantes**. Esta instrumentación añade trabajo a PostgreSQL y se
mantiene igual en la comparación; no es necesaria para la idempotencia ni se
habilita en la configuración de desarrollo o producción. Las marcas de commit
se consultan inmediatamente después de cada fase y se guardan en los artefactos:
PostgreSQL no las conserva indefinidamente, por lo que no deben tratarse como un
registro de auditoría permanente. Si falta una marca necesaria (`NULL`), la
medición falla en lugar de sustituirla por la hora de observación. Véase la
[documentación de timestamps de commit de PostgreSQL](https://www.postgresql.org/docs/17/functions-info.html#FUNCTIONS-COMMIT-TIMESTAMP).

Los comandos guardan `created_at` al registrarse por primera vez y `completed_at`
cuando terminan, también si fallan. Ambos utilizan el reloj de PostgreSQL;
`completed_at` se toma justo antes del commit y sirve como dato de diagnóstico
del comando, no como final principal del flujo. El resultado terminal y su
timestamp se confirman en la misma transacción que las escrituras de negocio. Los reintentos conservan los timestamps originales. Los
registros anteriores a esta instrumentación mantienen tiempos desconocidos
(`NULL`), sin inventar su historia. La auditoría conserva los timestamps y la
duración de cada comando junto al resultado de su flujo. Estas duraciones no
sustituyen la E2E: no incluyen, por ejemplo, el tiempo anterior al primer registro
ni las esperas entre comandos.

Para calcular E2E se combinan el inicio del cliente y el final de PostgreSQL: los
contenedores de este experimento comparten el reloj del host Docker. Trasladar el
generador o la base de datos a otra máquina exigiría sincronizar y comprobar sus
relojes. El límite de finalización en PostgreSQL es común a las dos variantes;
la entrega del resultado al cliente final queda fuera de esa latencia de negocio.

El goodput divide los flujos completos auditados entre el tiempo desde el inicio
de la carga hasta el mayor de estos límites: final del intervalo de carga
ofrecida, final del último intento registrado por k6 o última finalización
persistida de los flujos y comandos observada en la auditoría. Así incluye el trabajo que termina
posteriormente, sin añadir al denominador el tiempo administrativo de analizar
archivos o inspeccionar colas. La espera hasta la siguiente consulta del auxiliar
no alarga artificialmente la latencia del comando: se usa su timestamp persistido.
Los percentiles excluyen flujos sin completar y deben leerse junto a esos recuentos.

Se muestran los percentiles de cada repetición por separado: no se promedian
percentiles para obtener uno supuestamente global. Para decidir qué variante
mejora, busca una carga que cumpla las comprobaciones funcionales y los límites de
latencia y errores que tenga sentido exigir al producto. Un mayor número de
respuestas por segundo acompañado de operaciones incorrectas no es una mejora.

### Cómo interpretar los fallos

El evaluador distingue tres niveles que tienen denominadores diferentes:

| Medida | Cálculo | Qué significa |
|---|---|---|
| Peticiones HTTP fallidas | Fallidas clasificadas / total de peticiones iniciadas por el cliente. | Una petición no cumplió el estado HTTP y el cuerpo esperados. |
| Flujos sin completar | (Iniciados − completados auditados) / iniciados. | No se pudo verificar el resultado final del flujo en esta observación. |
| Operaciones descartadas | Descartadas / (iniciadas + descartadas). | k6 no tuvo capacidad para iniciar esas operaciones al ritmo solicitado; nunca llegaron a la API. |

Un flujo directo correcto realiza tres peticiones HTTP; en eventos realiza tres
POST y al menos dos GET de dependencias. Si falla un paso, el flujo se detiene y
no envía los siguientes: con errores, multiplicar los flujos iniciados por tres
no da el número de peticiones HTTP iniciadas. Las operaciones descartadas tampoco
son peticiones HTTP rechazadas por el servidor.

La auditoría reparte los flujos sin completar en categorías excluyentes:

| Estado | Qué se observó al auditar |
|---|---|
| `failed` | Un comando falló o los datos del flujo incumplen las comprobaciones. |
| `missing` | Falta al menos un comando que el cliente intentó enviar, sin un fallo terminal que tenga precedencia. |
| `pending` | Hay comandos registrados que siguen pendientes, sin comandos fallidos o ausentes. |
| `incomplete` | No se enviaron todos los pasos, o falta confirmación suficiente para reconstruir el resultado directo. |

`pending`, `missing` e `incomplete` describen lo que no pudo resolverse dentro del
experimento; **no prueban una pérdida permanente de mensajes**. Un comando
intentado ausente puede corresponder a una petición que nunca llegó a registrarse,
y un pendiente puede requerir más tiempo o una reentrega.

En `summary.json`, los contadores HTTP propios del evaluador son:

| Contador | Significado |
|---|---|
| `http_requests_total` | Peticiones iniciadas por el cliente; no acredita que hayan llegado al servidor. |
| `http_requests_succeeded` | Peticiones que cumplen estado y cuerpo esperados. |
| `http_requests_failed` | Peticiones terminadas que se clasificaron como fallidas. |
| `http_responses_4xx` | Fallos con respuesta HTTP `4xx`. |
| `http_responses_5xx` | Fallos con respuesta HTTP `5xx`. |
| `http_transport_errors` | Fallos sin respuesta HTTP, registrados por k6 con estado `0`. |
| `http_timeouts` | Timeouts de petición (`1050`) o conexión (`1211`) identificados por k6; son un subconjunto de los errores de transporte. |
| `http_unexpected_responses` | Otros estados inesperados o respuestas con cuerpo inválido, incluso si son `2xx`. |

Las cuatro categorías de fallo son excluyentes: `4xx + 5xx + transporte +
inesperadas = fallidas`. No se vuelven a sumar los timeouts. El contador nativo
`http_req_failed` de k6 aplica su criterio HTTP; los contadores anteriores añaden
la validación del contrato de nuestra API. En las pruebas de fiabilidad, los
`404` y `409` esperados son resultados correctos; se evalúan por separado de la
carga, cuyas operaciones esperan respuestas satisfactorias.

El informe también muestra `http_requests_unfinished`: iniciadas menos correctas
menos fallidas clasificadas. Permite detectar peticiones que no llegaron a
clasificarse, por ejemplo si se interrumpió su ejecución. No se cuentan como
correctas ni se ocultan dentro de otro tipo de fallo. Si este número es mayor
que cero, el porcentaje de fallos clasificados no describe todos los intentos.
En el CSV, `http_error_percent` usa las peticiones iniciadas como denominador;
`failed_workflows` y `workflow_error_percent` describen los flujos incompletos.

**Fallo HTTP no equivale a pérdida de datos.** Una escritura puede guardarse y
su respuesta no llegar al cliente. `unknown_writes` conserva las escrituras cuyo
resultado no se pudo confirmar en la respuesta. Si la auditoría encuentra el
flujo completo y sus datos de negocio correctos, cuenta como completado aunque
el POST final haya respondido con error o haya agotado su timeout. En ambas
variantes exige los tres comandos completados, con resultados y filas de
negocio consistentes. Su latencia se calcula con el commit persistido. Los
contadores HTTP y la incertidumbre observada por el cliente no se borran y **la
ejecución sigue fallando sus comprobaciones de carga**. Completar el trabajo más
tarde no convierte una experiencia HTTP fallida en satisfactoria.

La auditoría también puede atribuir una creación directa cuya respuesta se perdió
mediante su `command_id`. Eso no completa el flujo si el cliente no pudo obtener
la entrega para continuar: se conserva como incompleto, con el comando y la fila
correspondientes. Contar filas sin relacionarlas con los comandos no basta para
demostrar ausencia de pérdidas o duplicados.

## Resultados

Cada experimento crea una carpeta en `benchmarks/results/<timestamp-id>/`:

```text
manifest.json          Configuración, entorno, versiones y huellas del código
comparison.csv         Una fila por medición, para comparar repeticiones
report.md              Resumen legible
reliability-*.json      Conflictos y solicitudes repetidas, por adaptador
orchestrator.log        Comandos ejecutados y su salida
<ejecución>/
    configuration.json Configuración de esta medición
    summary.json       Mediciones y comprobaciones de k6
    result.json        Métricas normalizadas y estado de esta fase
    receipts.log       Un recibo por flujo, con tiempos e IDs intentados/confirmados
    verification.json  Flujos, comandos, datos y latencias reconciliados
    resources.jsonl    Muestras de recursos de los contenedores
    resource-coverage.json  Número de muestras por servicio y avisos
    k6.log             Salida del generador
    services.log       Logs recientes de API y PostgreSQL
    drain.json         Comprobación de cierre de conexiones antes de auditar
    command-drain.json Espera acotada del trabajo pendiente, solo en eventos
    queues.json        Mensajes y consumidores de ambas colas, solo en eventos
    warmup/            Los mismos artefactos de fase, para el calentamiento
```

El proceso devuelve un código distinto de cero si fallan las comprobaciones de
la medición, la fiabilidad o la auditoría de la medición. Un calentamiento fallido
se conserva en los resultados, pero por sí solo no hace fallar una medición
posterior correcta. Un fallo de los umbrales de carga es un
resultado del experimento: se conservan los recuentos y se continúa con el resto
de la matriz. Si falla la comprobación funcional previa de fiabilidad, se omite
la medición con estado `reliability_failed`; sus métricas figuran como `N/A`
en el informe y quedan vacías en el CSV, no como ceros. Un fallo de infraestructura
o no poder asegurar que no quedan operaciones en PostgreSQL detiene el
experimento. Se conservan resultados y logs; si no llega a generarse una tabla,
consulta `manifest.json` y `orchestrator.log`.

El informe muestra mediciones, comandos y colas, consumo de CPU, desglose de
fallos HTTP y calentamientos con sus propias auditorías y recuperación. En el CSV,
los campos `warmup_*` resumen el calentamiento y `warmup_recovery` indica si hubo
que reiniciar la API. Un calentamiento fallido no se confunde con una medición
omitida ni sus contadores se suman a los de los segundos de carga medidos.

El manifiesto, cada `configuration.json` y el CSV incluyen `execution_model: "async"`.
Cada ejecución identifica además `processing_model` (`direct` o `queued`), el
adaptador de persistencia y los límites de CPU y procesos asignados. El informe
también identifica ese modelo. Los artefactos de experimentos anteriores
no se modifican: `postgres` y `postgres_pool` representaban implementaciones
síncronas en versiones anteriores del código. Para interpretarlos, consulta su
commit, las huellas del código y la configuración guardada; la ausencia de esta
nueva etiqueta no permite deducir por sí sola cómo se ejecutó una medición.
El manifiesto añade `result_format_version: 5` y `summary.json` incluye
`benchmark.version: 5`, el inicio y la duración de la carga ofrecida. El formato
v5 añade idempotencia y auditoría de comandos a la ejecución directa. **Los
resultados anteriores no son comparables directamente**: v4 medía la directa sin
registro idempotente, y v3 además consultaba el final por HTTP. Los artefactos
anteriores se conservan intactos; no se presentan como mediciones de este cambio.
La auditoría mantiene compatibilidad con los recibos v4 históricos.

`resource_warnings` en el CSV indica avisos de muestreo; su detalle está en
`resource-coverage.json`. La ausencia de muestras no significa consumo cero ni
invalida automáticamente las comprobaciones HTTP y de datos.

La validación de carga exige que todas las comprobaciones de respuesta pasen,
que se envíe al menos un flujo hasta el último paso y que no haya errores técnicos,
escrituras de resultado desconocido ni iteraciones descartadas. La auditoría
posterior exige flujos completos y correctos; agotar la espera de comandos
pendientes también hace fallar la fase. Todavía no se impone un límite de latencia: que una ejecución pase estas comprobaciones no
significa que cumpla un objetivo de tiempo de respuesta del producto.

Para probar el propio evaluador sin generar carga, si `api/.venv` tiene las
dependencias de `api/requirements.txt` (incluidos pytest y psycopg):

```bash
api/.venv/bin/python -m pytest -q benchmarks/tests
```

Opcionalmente, si tienes Node.js, puedes comprobar la clasificación de respuestas
y los contadores del escenario k6 sin generar carga ni instalar paquetes:

```bash
node --test benchmarks/tests/load.test.mjs
```

Node.js solo es necesario para esas pruebas, no para ejecutar el evaluador.

## Límites de esta primera versión

La máquina comparte recursos entre aplicación, base de datos y generador. Sirve
para comparar variantes en condiciones locales controladas; a cargas altas hay
que comprobar si k6 dispone de suficientes recursos y, si hace falta, trasladarlo
a otra máquina. Ejecuta una evaluación a la vez, mantén constantes los límites
de Docker y evita otras tareas pesadas durante las mediciones. La imagen local
de la API se comparte entre experimentos; `--no-build` reutiliza esa imagen y
no incorpora los cambios de código posteriores a su construcción. Antes de generar
carga, el evaluador comprueba que las rutas y operaciones de los repositorios de
la imagen son asíncronas. Si has conservado una imagen síncrona antigua, vuelve a
ejecutar sin `--no-build` para reconstruirla.

Las muestras de `docker stats` no permiten observar cada pico breve. Esta versión
tampoco recoge todavía las estadísticas internas del pool ni las esperas dentro
de PostgreSQL. Las pruebas cortas de conflictos y repetición comprueban casos
concretos; no demuestran corrección bajo todas las carreras posibles.

Un timeout puede ocurrir después de que PostgreSQL confirme una escritura. El
generador no reintenta automáticamente las peticiones fallidas. Los identificadores
de comandos y los intentos de devolución permiten recuperar resultados bajo las
condiciones descritas arriba. Una finalización tardía puede aumentar los flujos
completos auditados, pero mantiene el error HTTP y el fallo de la ejecución.

La cola se inspecciona al terminar cada fase, tras detener los procesos; no se
mide continuamente su máximo tamaño ni se cuenta cada reentrega. Los comandos
pendientes no se dejan ejecutar indefinidamente: la espera posterior tiene un
límite, se conserva su resultado y el trabajo restante se elimina durante el
reset aislado. Acabar la generación de carga no significa que se haya vaciado
la cola. Las latencias usan los timestamps de finalización y el goodput incluye
el intervalo de carga y el trabajo observado que termina después; no se calculan
solo sobre las respuestas `202` ni sobre los comandos que el cliente alcanzó a
consultar.

Persisten dos consultas mínimas de dependencias por flujo asíncrono, y más si el
worker todavía no ha terminado. Eliminarlas requeriría cambiar cómo se encadenan
las operaciones o se comunican sus resultados. Este evaluador reduce la
observación innecesaria y hace explícito el coste que todavía tiene el protocolo.

Quedan para siguientes experimentos los cortes de conexión, la recuperación tras
fallos, cargas sostenidas más largas, varios consumidores y la comparación con
ORM. Una cola puede absorber picos y permitir reintentos, pero estas mediciones
por sí solas no demuestran recuperación ante todos los fallos ni ausencia de
pérdidas bajo cualquier condición.

Para profundizar, la documentación de k6 explica el
[modelo abierto de carga](https://grafana.com/docs/k6/latest/using-k6/scenarios/concepts/open-vs-closed/)
y los [resúmenes al terminar una prueba](https://grafana.com/docs/k6/latest/results-output/end-of-test/).
