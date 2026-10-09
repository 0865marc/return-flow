# Evaluación de rendimiento y fiabilidad

Este evaluador compara `postgres` (una conexión por operación) con `postgres_pool`
(conexiones reutilizadas). Mantiene los mismos endpoints, consultas y reglas del
negocio: la variable que cambiamos es el adaptador de persistencia.

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

Esta prueba ejecuta ambos adaptadores y los tres escenarios, a 2 operaciones por
segundo, con 1 segundo de calentamiento, 3 de medición y una repetición. Comprueba
que el evaluador funciona; es demasiado corta para concluir qué adaptador rinde
mejor.

Después, ejecuta la comparación predeterminada del flujo completo:

```bash
python3 benchmarks/run.py
```

Evalúa `flow` con ambos adaptadores, a 10 y 50 operaciones por segundo, con
5 segundos de calentamiento, 30 de medición y tres repeticiones. Son 12
mediciones. Abre el `report.md` de la carpeta de resultados que imprime el script
para comparar cada repetición.

Para ampliar los escenarios y prolongar las mediciones:

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
| `--adapters` | `postgres postgres_pool` | Adaptadores que se comparan. |
| `--scenarios` | `flow` | Escenarios; admite `read`, `create` y `flow`. |
| `--rates` | `10 50` | Operaciones iniciadas por segundo, no peticiones HTTP. |
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
escritos durante el calentamiento no cambien el punto de partida. El proceso de
la API y su pool permanecen calientes. Se alterna el orden de los adaptadores
entre repeticiones para reducir el efecto de ejecutarlos siempre en el mismo
orden.

k6 intenta iniciar operaciones a un ritmo fijo. Así, cuando la API se ralentiza,
el cliente no reduce automáticamente la carga por esperar las respuestas. Si no
puede iniciar alguna operación, se registra como una iteración descartada. Una
ejecución con descartes no demuestra que la API haya sostenido el ritmo solicitado.

Se conservan por ejecución:

- Operaciones terminadas correctamente por segundo (*goodput*) y peticiones HTTP
  realizadas. Un flujo solo cuenta como correcto si supera todas sus comprobaciones.
  El tiempo utilizado para calcular el goodput incluye la espera final para que
  terminen las operaciones que ya estaban en curso.
- Latencias p50, p95 y p99 por endpoint y de los flujos completados correctamente.
  Por ejemplo, p95 es el tiempo dentro del cual termina el 95 % de las operaciones
  medidas. Las latencias de los flujos correctos deben leerse junto a los errores;
  excluyen los flujos fallidos.
- Errores, comprobaciones fallidas, timeouts e iteraciones descartadas.
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
    receipts.log       Identificadores confirmados para la auditoría
    verification.json
    resources.jsonl    Muestras de recursos de los contenedores
    resource-coverage.json  Número de muestras por servicio y avisos
    k6.log             Salida del generador
    services.log       Logs recientes de API y PostgreSQL
    warmup/            Resultados del calentamiento, separados de la medición
```

El proceso devuelve un código distinto de cero si fallan las comprobaciones de
carga, de fiabilidad o la auditoría. Si fallan la fiabilidad o el calentamiento,
se omite la medición con estado `reliability_failed` o `warmup_failed`: no se
reinician los datos mientras puedan quedar escrituras en curso. Se continúa con
el resto de la matriz; un fallo de infraestructura sí detiene el experimento.
Se conservan resultados y logs; si no llega a generarse una tabla, consulta
`manifest.json` y `orchestrator.log`.

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

## Límites de esta primera versión

La máquina comparte recursos entre aplicación, base de datos y generador. Sirve
para comparar variantes en condiciones locales controladas; a cargas altas hay
que comprobar si k6 dispone de suficientes recursos y, si hace falta, trasladarlo
a otra máquina. Ejecuta una evaluación a la vez, mantén constantes los límites
de Docker y evita otras tareas pesadas durante las mediciones. La imagen local
de la API se comparte entre experimentos; `--no-build` reutiliza esa imagen y
no incorpora los cambios de código posteriores a su construcción.

Las muestras de `docker stats` no permiten observar cada pico breve. Esta versión
tampoco recoge todavía las estadísticas internas del pool ni las esperas dentro
de PostgreSQL. Las pruebas cortas de conflictos y repetición comprueban casos
concretos; no demuestran corrección bajo todas las carreras posibles.

Un timeout puede ocurrir después de que PostgreSQL confirme una escritura. Si el
cliente no recibió su identificador, el resultado es desconocido: no podemos
dar por válido ese caso de fiabilidad solo con el recibo de las respuestas. Para
estudiar reintentos y efectos duplicados bajo esos fallos necesitaremos un
identificador de operación y un contrato de idempotencia.

Quedan para siguientes experimentos los cortes de conexión a PostgreSQL, la
recuperación tras fallos, cargas sostenidas más largas y la comparación con
adaptadores asíncronos, ORM o eventos. Para eventos habrá que medir por separado
el tiempo de aceptación y el tiempo hasta completar y persistir la operación.

Para profundizar, la documentación de k6 explica el
[modelo abierto de carga](https://grafana.com/docs/k6/latest/using-k6/scenarios/concepts/open-vs-closed/)
y los [resúmenes al terminar una prueba](https://grafana.com/docs/k6/latest/results-output/end-of-test/).
