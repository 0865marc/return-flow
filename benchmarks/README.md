# Benchmark de dos fases

Este benchmark compara `postgres_pool` y `event_driven` separando la generación de carga de la comprobación de resultados. Durante la ejecución no consulta estados de comandos, entregas o devoluciones, no calcula percentiles y no hace polling. La medición comienza cuando la API y el worker ya están detenidos.

La separación elimina la carga de observación concurrente del benchmark anterior. No significa un coste absoluto de cero: enviar HTTP, guardar recibos en memoria y registrar timestamps consume recursos, y los contenedores comparten el host. Conviene ejecutar la comparación en un host sin otras cargas relevantes.

Los [resultados del caso de estudio](../docs/performance.md) documentan las
mediciones y sus límites. El ensayo de alta carga con reparto de 3 CPU para la
API directa frente a 2 CPU de API + 1 de worker utiliza una
[receta adicional con k6](../docs/experiments/three_cpu/README.md).

## Ejecución rápida

Desde la raíz del repositorio, con Python 3 y Docker Compose disponibles:

```bash
python3 benchmarks/run.py --smoke
```

El runner construye la imagen de aplicación, prepara la infraestructura y ejecuta seis casos: los dos adaptadores por las tres operaciones. Cada caso envía carga durante 2 segundos a 5 peticiones/s y después espera 1 segundo sin consultar el backend. `--smoke` fija esos valores y una repetición; respeta los filtros de adaptadores y operaciones.

Una comprobación de un solo caso:

```bash
python3 benchmarks/run.py --smoke --adapters event_driven --operations return
```

Una comparación con carga mayor:

```bash
python3 benchmarks/run.py \
  --adapters postgres_pool event_driven \
  --operations create deliver return \
  --rates 50 200 \
  --duration 30 \
  --repetitions 3 \
  --concurrency 64 \
  --drain-seconds 30
```

No hay que arrancar el `docker-compose.yml` de desarrollo. Cada caso usa un proyecto `rfbench-…`, red y volúmenes nuevos. Al terminar se eliminan únicamente los recursos de ese proyecto; los JSON del host se conservan. Desde el arranque se utiliza una copia resuelta de Compose por caso, también para la limpieza. Los casos se ejecutan secuencialmente y el orden de los adaptadores se invierte en las repeticiones pares.

## Preparación y fases

Antes de iniciar la ventana de ejecución se generan el plan completo, sus identidades y los datos de partida. Se crea el esquema, se espera a que estén disponibles PostgreSQL, RabbitMQ cuando corresponde y la API, y se envía **un único comando de calentamiento**. Su finalización se comprueba mediante polling durante esta preparación. Estas comprobaciones terminan antes de medir; el comando de calentamiento tiene otra identidad y queda fuera de las analíticas.

1. **Ejecución, sin observación del backend.** El cliente envía exclusivamente los POST planificados durante `--duration`. Después espera las respuestas HTTP que siguen abiertas, permanece inactivo durante `--drain-seconds` y el runner detiene la API y el worker. El tiempo de espera pasiva es fijo: no se consulta si la cola está vacía ni si los comandos han terminado. Los recibos se mantienen en memoria durante el envío y la espera pasiva; `execution.json` se escribe después de ambos. La parada define el límite de trabajo que se observará.
2. **Medición.** Una vez verificada la parada de los procesos de aplicación, se obtiene un snapshot de PostgreSQL en una transacción `REPEATABLE READ READ ONLY`, leyendo por lotes de hasta 1.000 comandos. Se comprueba también el estado real de las entidades de negocio. A continuación se reconcilian los recibos y el snapshot y se calculan las analíticas localmente, sin nuevas peticiones HTTP a la aplicación.

El snapshot puede incluir varios SELECT; no hay un bucle que espere estados terminales. La API y el worker no pueden competir con esas lecturas porque ya se han detenido. Los healthchecks de Docker están desactivados en este Compose y la API se ejecuta sin access log ni recarga automática.

La parada puede cancelar operaciones activas o devolver mensajes sin confirmar a RabbitMQ. No garantiza vaciar la cola. Un comando `pending`, ausente o desconocido queda **sin resolver al corte**: su tiempo de finalización está censurado y no se inventa una latencia ni se interpreta como pérdida demostrada. En el flujo encolado, el worker crea el comando en la misma transacción que el negocio y su resultado: un comando aceptado puede no tener registro visible porque sigue en RabbitMQ o porque su transacción no llegó a confirmarse. El snapshot de PostgreSQL no distingue esas situaciones. Si interesa observar más finalizaciones, se repite el caso con un `--drain-seconds` mayor.

## Escenarios independientes

Cada petición tiene un `command_id` UUID5 determinista dentro de su `run_id`, enviado como `Idempotency-Key`. Las operaciones que lo necesitan tienen una entrega distinta por petición, creada durante la preparación.

| Operación | Estado inicial | POST directo / encolado | Comprobación de negocio |
| --- | --- | --- | --- |
| `create` | No necesita fixture | `/deliveries` / `/deliveries-async` | Existe la entrega creada con estado `pending`. |
| `deliver` | Una entrega `pending` por petición | `/deliveries/{id}/deliver` / `/deliveries/{id}/deliver-async` | La entrega indicada queda `delivered`. |
| `return` | Una entrega `delivered` por petición | `/returns` / `/returns-async` | Existe una devolución `requested` ligada a esa entrega; la entrega sigue `delivered`. |

Son escenarios de una operación sobre datos independientes. No representan un flujo encadenado de crear, entregar y devolver, ni miden la duración completa de ese recorrido. Preparar fixtures mediante SQL evita introducir peticiones previas de negocio en la ventana medida.

## Generador de carga

La tasa es abierta: una respuesta lenta no retrasa deliberadamente la planificación de la siguiente petición. El plan contiene `ceil(rate × duration)` entradas y los envíos se espacian según la tasa solicitada. Los paths, cuerpos y headers se preparan antes de arrancar el reloj de envío.

Cada worker del cliente mantiene una conexión HTTP persistente. Se priorizan las conexiones libres usadas más recientemente y se renuevan las que llevan 4 segundos ociosas, antes del cierre por keepalive de la API; esa renovación sucede antes de enviar una petición y no supone un reintento. `--concurrency` limita tanto las peticiones asignadas como las que están en curso; no existe una cola ilimitada. Los slots que no se pueden ejecutar se conservan en los recibos como `dropped`:

| Motivo | Significado |
| --- | --- |
| `capacity` | Todos los workers del cliente estaban ocupados. |
| `generator_lag` | El planificador o un worker no llegó a tiempo al slot. |
| `duration_elapsed` | La entrada del plan quedaba fuera de la ventana de envío. |

El generador no recupera slots perdidos con ráfagas y no reintenta errores HTTP, desconexiones o timeouts. Por tanto, la tasa solicitada puede ser distinta de la carga efectivamente enviada. Los descartes se informan explícitamente y hacen que un caso no pueda aprobar como completo.

Los recibos contienen las identidades, el instante relativo de inicio, la duración HTTP, el estado HTTP y el cuerpo sin analizar, limitado a 4.096 bytes, además de los errores de transporte. El análisis de JSON y los percentiles se calculan en la segunda fase. `--timeout` es el timeout de las operaciones de socket de `http.client`; no es un deadline total de petición.

## Recursos y configuración

La aplicación usa un presupuesto total de CPU comparable por defecto:

| Recurso | `postgres_pool` | `event_driven` |
| --- | --- | --- |
| API | 2 CPU; 2 procesos Uvicorn | 1 CPU; 1 proceso Uvicorn |
| Worker | No se inicia | 1 CPU; 1 proceso |
| PostgreSQL | 1 CPU | 1 CPU |
| RabbitMQ | No se inicia | 1 CPU |
| Cliente | 1 CPU | 1 CPU |

`--app-cpus` se asigna íntegramente a la API directa y se divide por igual entre API y worker en el modo encolado. El número de procesos Uvicorn es `max(1, ceil(CPU de la API))`. El broker dispone de presupuesto adicional: la igualdad de CPU de aplicación no implica igualdad del total de infraestructura.

`--pool-size` es el máximo **por proceso**, no un límite global de conexiones. Con los valores iniciales, la API directa dispone de dos pools de 10 conexiones; el modo encolado tiene un pool de 10 en la API y otro de 10 en el worker. Los POST encolados no utilizan el pool de la API: solo publican y esperan la confirmación de RabbitMQ; el worker registra y ejecuta cada comando con su propio pool. La API mantiene su pool para las rutas directas y las consultas. La concurrencia de negocio del worker es independiente de la concurrencia HTTP del cliente.

| Opción | Valor inicial |
| --- | --- |
| `--adapters` | `postgres_pool event_driven` |
| `--operations` | `create deliver return` |
| `--rates` | `10 50 200` peticiones/s |
| `--duration` | 15 s de envío |
| `--repetitions` | 3 |
| `--concurrency` | 64 |
| `--timeout` | 10 s por operación de socket |
| `--drain-seconds` | 15 s de espera pasiva después del drenaje HTTP |
| `--stop-timeout` | 10 s de gracia para la parada de contenedores |
| `--app-cpus` | 2 |
| `--db-cpus` / `--broker-cpus` / `--client-cpus` | 1 / 1 / 1 |
| `--pool-size` | 10 por proceso; timeout de adquisición de 5 s |
| `--worker-concurrency` | 8 |
| `--output` | `benchmarks/results` |

La matriz sin argumentos contiene **54 casos**: 2 adaptadores × 3 operaciones × 3 tasas × 3 repeticiones. Su duración incluye preparación, envío, drenajes, parada y medición de cada caso.

El Compose usa `postgres:17-alpine`, `rabbitmq:4-alpine` y una imagen de aplicación compartida `return-flow-benchmark-v2:local`, construida desde `api/`. No publica puertos en el host ni reutiliza contenedores o volúmenes de desarrollo. La imagen de aplicación se construye una vez al principio; `--no-build` exige que ya exista y permite reutilizarla. Las etiquetas de infraestructura pueden cambiar con el tiempo; se guardan las imágenes e identidades utilizadas y la configuración Compose resuelta para revisar cada ejecución.

## Cómo leer las métricas

`report.json` separa aceptación HTTP, finalización persistida e integridad del resultado. Un `202` válido demuestra aceptación confirmada por RabbitMQ, no finalización ni existencia de un registro visible en PostgreSQL. También puede haber comandos completados pese a un timeout o error HTTP: el informe mantiene ambos hechos por separado.

| Métrica | Definición y alcance |
| --- | --- |
| `latency_ms.http` | Duración del intento HTTP con el reloj monotónico del cliente. Incluye intentos con error. En el modo encolado mide la respuesta de aceptación; no mide la finalización del comando. |
| `latency_ms.db_completed` | `completed_at - created_at` para comandos completados, usando únicamente timestamps de PostgreSQL. |
| `latency_ms.db_failed` | El mismo intervalo para comandos que acabaron con un fallo persistido. Se informa por separado. |
| `throughput.http_requests_per_s` | Peticiones enviadas divididas por la ventana HTTP real: envío más espera de respuestas abiertas. |
| `throughput.completed_commands_per_s` | Comandos completados divididos por `processing_duration_s`. |
| `throughput.business_valid_completed_commands_per_s` | Comandos completados cuya integridad de negocio se comprobó, con el mismo denominador. |

El intervalo de base de datos va desde el registro del comando hasta la escritura de su resultado terminal. En `event_driven`, el registro lo crea el worker al ejecutar: **no incluye la espera en RabbitMQ**, la publicación ni la espera inicial por una conexión del pool. `completed_at` se toma **antes de COMMIT**: marca un punto anterior a la confirmación y omite su coste posterior. No es el instante exacto de commit ni una latencia de extremo a extremo desde el cliente. No se restan timestamps del cliente a timestamps de la base de datos. Un intervalo de base de datos corto no demuestra que el comando terminara pronto desde su aceptación.

Los resultados históricos donde la API registraba el comando antes de publicarlo incluían la espera posterior en RabbitMQ dentro de este intervalo. Esa métrica no es directamente comparable con la del flujo actual; hay que revisar la versión de la aplicación utilizada en cada ejecución.

`processing_duration_s` empieza antes de lanzar el contenedor de ejecución y termina después de la parada y su comprobación. Incluye la orquestación de ese contenedor, el envío, el drenaje HTTP, la espera pasiva, la escritura del recibo y la parada. Excluye la preparación y la fase de medición. Es un throughput observado sobre ese intervalo completo, no una estimación de capacidad sostenida basada exclusivamente en los segundos de envío.

Los percentiles p50, p95 y p99 usan interpolación lineal en `(n - 1) × percentil`. Se acompañan del número de muestras, mínimo, media, máximo y exclusiones. Los timestamps ausentes, inválidos o con diferencias negativas se excluyen. Los comandos pendientes o ausentes no reciben tiempos de finalización artificiales: los percentiles de finalización describen solo las muestras observadas y pueden resultar optimistas cuando queda trabajo pendiente.

`execution` conserva los tiempos de envío, drenaje HTTP y duración HTTP total. `lifecycle` añade preparación, espera pasiva, parada y medición. Los campos de duración son monotónicos; las marcas ISO UTC permiten situar temporalmente el caso.

## Artefactos y resultado

Cada invocación crea una carpeta con timestamp y un sufijo único en `--output`. `summary.json` resume los casos que llegaron a producir un informe. Dentro de cada carpeta de caso se guardan:

| Archivo | Contenido |
| --- | --- |
| `config.json` | Identidad, escenario, tasa, límites y parámetros efectivos. |
| `plan.json` | Operaciones e identidades preparadas. |
| `compose.resolved.yml` | Configuración de contenedores y recursos resuelta. |
| `images.txt` | Imágenes utilizadas por los servicios. |
| `execution.json` | Recibos de todos los slots, descartes y tiempos de ejecución. |
| `snapshot.json` | Comandos persistidos del plan y resultado de la comprobación de negocio. |
| `lifecycle.json` | Tiempos y marcas de las fases. |
| `report.json` | Reconciliación, latencias, throughput, integridad y configuración. |
| `error.json` | Diagnóstico y ciclo de vida parcial si el caso se interrumpe por un error. |

En `report.json`, `pass` requiere que todas las peticiones previstas se envíen, tengan respuesta válida, terminen y superen la comprobación de integridad. `incomplete` indica descartes o resultados sin resolver al corte. `failed` indica errores HTTP, fallos persistidos o inconsistencias de integridad; tiene prioridad sobre `incomplete` si concurren ambas condiciones.

Los códigos de salida normales del runner son:

- `0`: todos los casos tienen resultado `pass`.
- `1`: al menos un caso tiene resultado `incomplete` o `failed`.
- `2`: error de argumentos, infraestructura u orquestación que impide completar la ejecución.

Un caso sobrecargado puede acabar como `incomplete`; esto permite distinguir el incumplimiento de la carga o el límite temporal de un fallo del benchmark.

## Pruebas locales

```bash
python3 -m unittest discover -s benchmarks/tests -v
```

Estas pruebas usan únicamente la biblioteca estándar de Python. No necesitan Docker, PostgreSQL ni RabbitMQ; las pruebas HTTP levantan un servidor falso en localhost y requieren que el entorno permita sockets locales. Comprueban ausencia de GET/polling y reintentos, contratos de POST, reutilización de conexiones, límites de concurrencia, descartes por retraso, captura acotada y reconciliación de resultados y métricas.
