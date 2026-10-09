# Evaluación de rendimiento y fiabilidad

Este evaluador compara `postgres` (una conexión asíncrona por operación) y
`postgres_pool` (pool de conexiones asíncronas). Ambas variantes comparten rutas,
casos de uso y puertos con `async/await`, además de las mismas consultas y reglas
del negocio. Solo cambia cómo se obtienen y liberan las conexiones a PostgreSQL.

Se ejecuta desde la terminal y guarda archivos locales. No necesita un servidor
Grafana, una cuenta ni un servicio permanente de monitorización. k6 es el programa
que genera las peticiones HTTP; lo ejecutamos en un contenedor temporal.

## Cómo encajan las piezas

```text
python3 benchmarks/run.py
    │
    ├── Arranca un entorno de evaluación con Docker Compose
    │       ├── FastAPI con el adaptador seleccionado
    │       └── PostgreSQL con una base de datos dedicada
    │
    ├── Prepara datos y comprueba conflictos y solicitudes repetidas
    ├── Ejecuta k6 ── HTTP ──► FastAPI ── SQL ──► PostgreSQL
    ├── Recoge consumo de recursos y comprueba los datos guardados
    ├── Repite con el siguiente adaptador
    └── Guarda los resultados y elimina los contenedores de evaluación
```

`run.py` coordina el experimento desde tu máquina usando la biblioteca estándar
de Python. k6 mide la experiencia del cliente. Los auxiliares de preparación y
verificación se ejecutan dentro del contenedor de la API, aprovechando psycopg.

El entorno usa `benchmarks/compose.yml` con un nombre de proyecto único
`return-flow-bench-<id>`. Su base de datos es `return_flow_benchmark`: no utiliza
los puertos, los datos ni los volúmenes del entorno de desarrollo. Cada ejecución
crea un volumen propio para PostgreSQL y lo elimina antes de la siguiente; al
terminar el experimento también se elimina. Los resultados permanecen en el disco
de tu máquina.

Cada contenedor tiene un límite de 1 CPU y 512 MiB de memoria. La API arranca con
un proceso, sin recarga automática y sin registrar cada petición HTTP. No se
necesita RabbitMQ para esta comparación.

## Ejecutar

Requiere Python 3.10 o posterior y acceso a Docker con Docker Compose. No necesitas
instalar k6 ni dependencias Python adicionales en tu máquina. La primera ejecución
descarga las imágenes y construye la API.

Desde la raíz del proyecto, empieza por una comprobación corta:

```bash
python3 benchmarks/run.py --smoke
```

Esta prueba ejecuta los dos adaptadores y los tres escenarios, a 2 operaciones por
segundo, con 1 segundo de calentamiento, 3 de medición y una repetición. Comprueba
que el evaluador funciona; es demasiado corta para concluir qué adaptador rinde
mejor. Son seis mediciones.

Después, ejecuta la comparación predeterminada del flujo completo:

```bash
python3 benchmarks/run.py
```

Evalúa `flow` con los dos adaptadores, a 10, 50 y 400 operaciones por segundo, con
5 segundos de calentamiento, 30 de medición y tres repeticiones. Son 18
mediciones previstas. También se miden las configuraciones que incumplan los
umbrales durante el calentamiento. Abre el `report.md` de la carpeta de resultados
que imprime el script para comparar cada repetición.

La selección explícita equivalente es:

```bash
python3 benchmarks/run.py \
  --adapters postgres postgres_pool \
  --rates 10 50 400 \
  --warmup 5 \
  --duration 30 \
  --repetitions 3
```

Se conservan los mismos límites de recursos y un proceso de API. El pool mantiene
una conexión como mínimo y permite configurar el máximo y el tiempo de espera.
Esta comparación mide el efecto conjunto de reutilizar conexiones y limitar
cuántas operaciones pueden utilizarlas a la vez; la variante sin pool abre una
conexión por operación y no impone ese límite adicional.

Para elegir otros ritmos, ampliar los escenarios y prolongar las mediciones:

```bash
python3 benchmarks/run.py \
  --adapters postgres postgres_pool \
  --scenarios read create flow \
  --rates 10 50 100 \
  --warmup 30 \
  --duration 120 \
  --repetitions 3
```

Cada combinación de adaptador, escenario, ritmo y repetición es una ejecución
independiente. El ejemplo anterior realiza 54 mediciones, además de sus
calentamientos: reserva tiempo suficiente.

| Argumento | Valor por defecto | Qué controla |
|---|---|---|
| `--adapters` | `postgres postgres_pool` | Adaptadores asíncronos que se comparan. |
| `--scenarios` | `flow` | Escenarios; admite `read`, `create` y `flow`. |
| `--rates` | `10 50 400` | Operaciones iniciadas por segundo, no peticiones HTTP. |
| `--duration` | `30` | Segundos de medición por ejecución. |
| `--warmup` | `5` | Segundos de calentamiento antes de medir. |
| `--repetitions` | `3` | Repeticiones de cada combinación. |
| `--seed-size` | `1000` | Entregas iniciales del conjunto de datos. |
| `--pool-size` | `10` | Máximo de conexiones del adaptador con pool. |
| `--pool-timeout` | `5` | Segundos máximos para esperar una conexión del pool. |
| `--preallocated-vus` | `50` | Usuarios virtuales preparados al iniciar k6. |
| `--max-vus` | `200` | Límite de usuarios virtuales de k6. |
| `--request-timeout` | `5` | Tiempo máximo en segundos por petición HTTP. |
| `--no-build` | Desactivado | Reutiliza la imagen de la API ya construida. |

Un usuario virtual ejecuta una operación cada vez. k6 utiliza tantos como necesita
para mantener el ritmo solicitado, hasta el límite indicado. Si las respuestas
tardan más, hacen falta más usuarios concurrentes para mantener ese ritmo.

## Qué se prueba

| Escenario | Operación | Qué permite comparar |
|---|---|---|
| `read` | Consultar una entrega existente. | Lecturas y coste de obtener conexiones. |
| `create` | Crear una entrega pendiente. | Escrituras sobre entidades independientes. |
| `flow` | Crear, entregar, solicitar devolución y consultar el resultado. | El recorrido completo por HTTP, casos de uso y persistencia. |

Los ritmos representan operaciones completas por segundo. `read` y `create`
necesitan una petición por operación; `flow` necesita cuatro. Comparar 50 flujos
por segundo con 50 lecturas por segundo no equivale a enviar la misma carga HTTP.

Antes de generar carga se comprueba, una vez por adaptador, el contrato actual
de la aplicación:

- Al intentar entregar simultáneamente la misma entrega pendiente, una petición
  debe responder `200` y las demás `409`. Solo hay una transición de estado.
- Repetir la solicitud de devolución puede crear varias devoluciones para la
  misma entrega. Actualmente no hay idempotencia ni una restricción de devolución
  única. El evaluador comprueba ese comportamiento; no lo presenta como una
  garantía contra duplicados.
- Solicitar una devolución para una entrega pendiente o inexistente debe fallar
  con `409` o `404`, respectivamente, sin guardar una devolución.

## Metodología

Cada ejecución parte de datos preparados de nuevo. Se prepara el conjunto inicial
antes del calentamiento y se restaura antes de la medición, para que los datos
escritos durante el calentamiento no cambien el punto de partida. Se alterna el
orden de los adaptadores entre repeticiones para reducir el efecto de ejecutarlos
siempre en el mismo orden.

El calentamiento se registra y audita por separado. Incumplir sus umbrales no
impide medir: precisamente queremos observar los errores cuando una configuración
se satura. Antes de restaurar los datos se comprueba que puede hacerse sin que
queden operaciones anteriores pendientes:

- Si todos los flujos iniciados terminan con respuestas válidas, se conserva el
  proceso de la API y su pool caliente. Esto también se aplica cuando k6 haya
  descartado operaciones que no llegó a iniciar.
- Si hay errores, flujos incompletos o escrituras de resultado desconocido, se
  detiene la API y se espera a que desaparezcan sus conexiones a PostgreSQL.
  Entonces se audita el calentamiento, se reinicia la API y se preparan los datos
  de la medición. Se registra `warmup_recovery: "api_restarted"`: esta medición
  puede empezar con el proceso y el pool fríos, lo que hay que considerar al
  interpretar sus latencias.

Si la medición termina con errores o resultados incompletos, también se detiene
la API y se espera a que desaparezcan sus conexiones antes de auditar los datos.
Ese cierre queda fuera de las latencias y del tiempo usado para calcular el
rendimiento. Si no se puede confirmar que han terminado las operaciones en la
base de datos, se aborta el experimento en lugar de reinicializarla.

k6 intenta iniciar operaciones a un ritmo fijo. Así, cuando la API se ralentiza,
el cliente no reduce automáticamente la carga por esperar las respuestas. Si no
puede iniciar alguna operación, se registra como una iteración descartada. Una
ejecución con descartes no demuestra que la API haya sostenido el ritmo solicitado.

Se conservan por ejecución:

- Operaciones terminadas correctamente por segundo (*goodput*) y peticiones HTTP
  iniciadas por el cliente. Un flujo solo cuenta como correcto si supera todas
  sus comprobaciones.
  El tiempo utilizado para calcular el goodput incluye la espera final para que
  terminen las operaciones que ya estaban en curso.
- Latencias p50, p95 y p99 por endpoint y de los flujos completados correctamente.
  Por ejemplo, p95 es el tiempo dentro del cual termina el 95 % de las operaciones
  medidas. Las latencias de los flujos correctos deben leerse junto a los errores;
  excluyen los flujos fallidos.
- Peticiones HTTP correctas y fallidas, con recuentos y porcentajes, y el desglose
  de fallos por respuestas `4xx`, `5xx`, errores de transporte y respuestas
  inesperadas. Los timeouts se muestran como parte de los errores de transporte.
- Flujos iniciados, completados e incompletos, e iteraciones descartadas antes de
  llegar a la API. Los resultados del calentamiento aparecen por separado.
- Muestras de CPU y memoria de los contenedores de API, PostgreSQL y k6 mediante
  `docker stats`: el muestreo espera al arranque de k6 y deja dos segundos entre
  consultas; cada consulta también necesita tiempo.
- Resultado de verificar los datos persistidos contra los identificadores
  confirmados por la API.

Las escrituras confirmadas se anotan en un registro de recibos para poder
comprobarlas después. Esa instrumentación añade trabajo al generador y debe
mantenerse igual en todas las variantes. La verificación de la base de datos se
realiza después de la carga y queda fuera de las latencias HTTP.

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
| Flujos incompletos | (Iniciados − completados) / iniciados. | Un flujo comenzó, pero no terminó todos sus pasos correctamente. |
| Operaciones descartadas | Descartadas / (iniciadas + descartadas). | k6 no tuvo capacidad para iniciar esas operaciones al ritmo solicitado; nunca llegaron a la API. |

Un flujo correcto de `flow` realiza cuatro peticiones HTTP. Si falla un paso,
el flujo se detiene y no envía los siguientes: con errores, multiplicar los flujos
iniciados por cuatro no da el número de peticiones HTTP iniciadas. Las operaciones
descartadas tampoco son peticiones HTTP rechazadas por el servidor.

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
su respuesta no llegar al cliente. `unknown_writes` identifica de forma
conservadora escrituras cuyo resultado no se puede confirmar desde la respuesta.
La auditoría SQL comprueba los identificadores de las escrituras confirmadas,
también los de pasos correctos de un flujo que falló más tarde. Cuando hay
escrituras desconocidas, no se puede afirmar que no hubo pérdida o duplicación
solo contando las filas finales.

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
    receipts.log       Identificadores confirmados para la auditoría
    verification.json
    resources.jsonl    Muestras de recursos de los contenedores
    resource-coverage.json  Número de muestras por servicio y avisos
    k6.log             Salida del generador
    services.log       Logs recientes de API y PostgreSQL
    drain.json         Comprobación de conexiones, si fue necesario detener la API
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

El informe contiene tres tablas: mediciones, desglose de sus fallos HTTP y
calentamientos con sus propios fallos, auditorías y recuperación. En el CSV,
los campos `warmup_*` resumen el calentamiento y `warmup_recovery` indica si hubo
que reiniciar la API. Un calentamiento fallido no se confunde con una medición
omitida ni sus contadores se suman a los de los segundos de carga medidos.

El manifiesto, cada `configuration.json` y el CSV incluyen `execution_model: "async"`.
El informe también identifica ese modelo. Los artefactos de experimentos anteriores
no se modifican: `postgres` y `postgres_pool` representaban implementaciones
síncronas en versiones anteriores del código. Para interpretarlos, consulta su
commit, las huellas del código y la configuración guardada; la ausencia de esta
nueva etiqueta no permite deducir por sí sola cómo se ejecutó una medición.
El manifiesto añade `result_format_version: 2` para identificar este formato de
resultados; los artefactos de ejecuciones anteriores se conservan intactos.

`resource_warnings` en el CSV indica avisos de muestreo; su detalle está en
`resource-coverage.json`. La ausencia de muestras no significa consumo cero ni
invalida automáticamente las comprobaciones HTTP y de datos.

La validación de carga exige que todas las comprobaciones de respuesta pasen,
que se complete al menos una operación y que no haya errores técnicos,
escrituras de resultado desconocido ni iteraciones descartadas. Todavía no se
impone un límite de latencia: que una ejecución pase estas comprobaciones no
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

Un timeout puede ocurrir después de que PostgreSQL confirme una escritura. Si el
cliente no recibió su identificador, el resultado es desconocido: no podemos
dar por válido ese caso de fiabilidad solo con el recibo de las respuestas. Para
estudiar reintentos y efectos duplicados bajo esos fallos necesitaremos un
identificador de operación y un contrato de idempotencia. El generador actual
no reintenta automáticamente las peticiones fallidas.

Quedan para siguientes experimentos los cortes de conexión a PostgreSQL, la
recuperación tras fallos, cargas sostenidas más largas y la comparación con
ORM o eventos. Para eventos habrá que distinguir la aceptación duradera de una
solicitud y la finalización de su operación: medir ambas latencias, operaciones
completadas por segundo, trabajo pendiente, reentregas y efectos de duplicados.
Una cola puede absorber picos y permitir reintentos, pero su mera presencia no
garantiza que no haya pérdidas. Habrá que evaluar las confirmaciones, la
durabilidad y la idempotencia, y seguir las solicitudes aceptadas para distinguir
las completadas, las pendientes y las que no se pudieron recuperar. Acabar la
generación de carga no significa que se haya vaciado la cola.

Para profundizar, la documentación de k6 explica el
[modelo abierto de carga](https://grafana.com/docs/k6/latest/using-k6/scenarios/concepts/open-vs-closed/)
y los [resúmenes al terminar una prueba](https://grafana.com/docs/k6/latest/results-output/end-of-test/).
