# Rendimiento: recibir trabajo y completarlo son objetivos distintos

El experimento compara dos contratos sobre el mismo caso de uso de devoluciones. La ruta directa responde cuando termina la transacción; la ruta de eventos responde cuando RabbitMQ confirma la publicación y el worker ejecuta después. Una respuesta más rápida en eventos demuestra una recepción más rápida, no una devolución completada antes.

Los resultados del 12 de octubre de 2026 apuntan a una decisión de producto: la cola permite absorber un pico conservando una respuesta breve al cliente, a cambio de esperar por el resultado y gestionar trabajo pendiente. No demuestra que añadir mensajería acelere la operación de negocio.

## Qué se midió

Se ofrecieron **2.000 peticiones/s durante 15 segundos**, con una sola repetición por variante. Cada petición solicita una devolución sobre una entrega distinta, ya marcada como entregada durante la preparación. Las identidades de comando son únicas; esta carga mide operaciones independientes, no un flujo completo de creación, entrega y devolución ni una tormenta de reintentos.

Los casos se ejecutaron secuencialmente, con proyectos Docker, datos y volúmenes independientes. Después de esperar las respuestas HTTP abiertas hubo **15 segundos de espera pasiva** y se detuvieron API y worker. Solo entonces se consultó PostgreSQL y se comprobó la integridad de las devoluciones. La espera pasiva no garantiza vaciar la cola.

![Preparación, ejecución sin polling y medición posterior](diagrams/benchmark-phases.svg)

[Fuente Mermaid](diagrams/benchmark-phases.mmd).

No se consultaron estados de comandos durante la carga. Esto elimina la competencia que introducía el polling del benchmark anterior. El cliente sí consume recursos para enviar, registrar tiempos y conservar resultados. El generador Python versionado difiere del control posterior con k6: k6 también valida las respuestas y agrega contadores y muestras durante la carga; escribe su resumen al terminar HTTP. La reconciliación con el negocio se mantiene fuera de la ejecución en ambos casos. Los contadores de CPU se leyeron antes y después de k6, sin sondeo periódico.

## Comparación con 3 CPU de aplicación

| Recurso | Directa | Eventos |
| --- | --- | --- |
| API | 3 CPU, 3 procesos Uvicorn | 2 CPU, 2 procesos Uvicorn |
| Worker | No se inicia | 1 CPU, 1 proceso, concurrencia 8 |
| PostgreSQL | 1 CPU | 1 CPU |
| RabbitMQ | No se inicia | 1 CPU |
| Generador | k6 1.6.1, 4 CPU, 512 VUs | Igual |
| Pool PostgreSQL | Máximo 10 conexiones por proceso | Igual; los POST encolados no usan el pool de la API |
| Publicaciones pendientes de confirmación | No aplica | 64 por proceso de API |

El presupuesto igualado es **API + worker**, no toda la infraestructura: eventos añade RabbitMQ. Son cuotas de contenedores en un host compartido, no núcleos físicos aislados. La API no llevaba instrumentación de diagnóstico en esta comparación.

| Métrica | Directa | Eventos |
| --- | ---: | ---: |
| Respuestas válidas / 30.000 previstas | 19.719 | 30.000 |
| Respuestas válidas / segundo de envío | 1.314,6 | 2.000,0 |
| Peticiones previstas no enviadas | 10.281 | 0 |
| HTTP medio | 385,0 ms | 8,4 ms |
| HTTP p95 | 590,3 ms | 21,7 ms |
| Comandos con resultado terminal a los 15 s¹ | 19.241 | 11.686 |
| Completados válidos en el snapshot final | 19.719 | 26.329 |
| Aceptados sin resolver al detener la aplicación | 0 | 3.671 |
| Errores HTTP o fallos de negocio observados | 0 | 0 |

¹ Reconstruidos después de la parada, a partir de `completed_at`; no son una lectura de la longitud de RabbitMQ. El resultado terminal se marca antes del commit y el desfase entre relojes de cliente y base de datos no se calibró.

![Respuestas válidas por segundo en la comparación con 3 CPU](diagrams/acceptance-results.svg)

[Fuente Mermaid](diagrams/acceptance-results.mmd). La línea marca la carga ofrecida. La altura de eventos es la carga probada, no un techo descubierto. El denominador es la ventana de envío de 15 s; incluye respuestas que llegaron durante el drenaje HTTP. `http_req_duration` de k6 mide envío, espera y recepción; excluye bloqueo y establecimiento de conexión. El tiempo de pared medio registrado fue 384,8 ms y 8,6 ms, respectivamente.

Eventos recibió todo el pico, pero durante los primeros 15 segundos la directa había producido más resultados. Al terminar los envíos, **18.314 comandos de eventos todavía no tenían timestamp terminal**. Había 3.671 sin resolver después de la espera pasiva y la parada. La cola absorbió trabajo; no eliminó su coste.

En directa, todos los comandos enviados tenían timestamp terminal aproximadamente **15,38 segundos** después del inicio. En eventos **no se observó la finalización de todos los aceptados**: 30,36 segundos corresponde al último resultado observado, no al tiempo para vaciar la cola. Este último tiempo está censurado por la parada del experimento.

### Una anomalía del generador que se conserva en la evidencia

k6 programó 30.001 llegadas en el límite de la ventana para un plan de 30.000 comandos. En directa, la llegada extra quedó entre los descartes: el contador bruto es 10.282, frente a 10.281 peticiones del plan sin enviar. En eventos, la iteración extra activó la guarda de fin de plan **antes de hacer POST**: se enviaron exactamente 30.000 comandos y todos recibieron un `202` válido.

Ambos procesos k6 terminaron con **código 99** por umbrales incumplidos: descartes/carga incompleta en directa y una iteración fuera del plan en eventos. La evidencia conserva los contadores originales. La guarda no fue un error de la API; tampoco se presenta como una ejecución sin incidencias del generador.

## Cómo se localizó el límite de recepción

Antes de redistribuir CPU se probó eventos con API 1 CPU + worker 1 CPU. Los controles con k6 mantuvieron 512 VUs y la imagen de API sin instrumentación:

| Publicaciones simultáneas por proceso | Respuestas válidas/s | HTTP medio |
| --- | ---: | ---: |
| 64 | 1.504,1 | 337,8 ms |
| 256 | 1.304,7 | 390,2 ms |

Ampliar la ventana no aumentó la capacidad observada. Un diagnóstico separado, con instrumentación y generador Python, mostró dónde se desplazaba la espera:

| Tiempo medio | 64 plazas | 256 plazas |
| --- | ---: | ---: |
| Espera por una plaza del publicador | 157,4 ms | 0,002 ms |
| Espera para acceder al canal AMQP | 48,1 ms | 216,8 ms |
| Bloqueo del canal mantenido | 0,082 ms | 0,085 ms |
| Espera de confirmación después de encolar en el cliente AMQP | 4,60 ms | 4,65 ms |

La API consumía casi todo su núcleo y el broker aproximadamente 0,47 CPU. Las muestras de CPU repartían el coste entre HTTP, publicador, tareas AMQP y planificación del bucle de eventos. La evidencia apunta a **CPU del proceso de API como límite dominante de recepción** en esa configuración, no a que las confirmaciones de RabbitMQ tardasen cientos de milisegundos. El tiempo de acceso al canal incluye planificación de tareas; no equivale a CPU gastada dentro de su bloqueo. La espera de confirmación incluye transporte y planificación local, no solo procesamiento del broker.

En la comparación posterior con 3 CPU, la API de eventos usó aproximadamente **1,48 CPU** y el worker **0,94**. En directa, la API usó **2,16 CPU** y PostgreSQL **0,94 de su única CPU**, con throttling registrado. Esto es coherente con un límite en el worker para completar eventos y en PostgreSQL para la directa. Son diagnósticos de estas ejecuciones, no máximos universales ni una demostración causal aislada de cada componente. Los intervalos de CPU incluyen inicialización de k6 y drenaje HTTP, y excluyen la espera pasiva.

## El ensayo anterior de 2 CPU no es una curva de escalado

El primer ensayo a 2.000 peticiones/s utilizó el generador Python con **256 conexiones**, frente a los 512 VUs de k6 del ensayo de 3 CPU:

| Variante | Enviadas y aceptadas | Enviadas/s | No enviadas por capacidad | No enviadas por retraso del generador |
| --- | ---: | ---: | ---: | ---: |
| Directa: API 2 CPU | 19.218 | 1.281,2 | 8.146 | 2.636 |
| Eventos: API 1 CPU + worker 1 CPU | 22.222 | 1.481,5 | 6.045 | 1.733 |

Todos los comandos enviados terminaron correctamente después del drenaje. Las 256 conexiones eran un **límite del cliente**, no una configuración máxima de la API. Tanto ese límite como los retrasos del planificador recortaron la carga ofrecida realmente al servidor.

La ventaja observada de recepción pasa aproximadamente del 16 % al 52 % entre tablas, pero cambian el generador y su concurrencia: esos porcentajes **no prueban un factor de escalado**. El control intermedio de eventos con el mismo k6 permite una comparación más cercana: 1.504,1 respuestas/s con una CPU de API frente a 2.000/s con dos, manteniendo una CPU de worker y 64 publicaciones por proceso.

## Límites y reproducción

Una sola repetición corta permite descubrir comportamientos y descartar una hipótesis de ajuste; no estima variabilidad, capacidad sostenida, resistencia a fallos ni costes de producción. Los contenedores compartieron host y no se retiraron otras cargas de desarrollo. La carga de devolución es pequeña y usa datos independientes; resultados con contención sobre una misma entidad, llamadas a transportistas o mensajes mayores podrían cambiar.

`completed_at - created_at` mide únicamente el tramo registrado en PostgreSQL. En eventos, el worker crea `commands` al ejecutar: **la métrica excluye la espera en RabbitMQ**. Además, `completed_at` se escribe antes de `COMMIT`. No se usa esa duración para afirmar una latencia completa de cliente a resultado. Los percentiles de finalización que excluyen comandos sin resolver pueden resultar optimistas.

El [extracto de evidencia versionado](evidence/performance-2026-10-12.json) conserva configuración, contadores, tiempos, consumo de CPU, identidades de imagen y hashes de los informes originales. Los artefactos completos permanecen fuera de Git; sus nombres en el JSON son identificadores de procedencia, no enlaces públicos. Las imágenes usadas se registraron durante la ejecución. Los commits `8dceee0` y `df2b23b` archivaron después los cambios de aplicación y benchmark: no se presentan como un checkout capturado en el momento del experimento.

El runner estándar permite repetir el **diseño del ensayo de 2 CPU**, con resultados nuevos dependientes del host y de las imágenes construidas:

```bash
python3 benchmarks/run.py \
  --adapters postgres_pool event_driven \
  --operations return --rates 2000 --duration 15 --repetitions 1 \
  --concurrency 256 --timeout 10 --drain-seconds 15 --stop-timeout 10 \
  --app-cpus 2 --db-cpus 1 --broker-cpus 1 --client-cpus 4 \
  --pool-size 10 --worker-concurrency 8
```

La comparación de **3 CPU utilizó un harness adicional con k6**, publicado ahora como [receta experimental](experiments/three_cpu/README.md). Fija explícitamente API 3 CPU para directa y API 2 CPU + worker 1 CPU para eventos. `--app-cpus 3` en el runner estándar divide eventos en **1,5 + 1,5 CPU**, por lo que ese flag por sí solo no reproduce la configuración del resultado publicado.

```bash
python3 docs/experiments/three_cpu/run_pair.py
python3 docs/experiments/three_cpu/completion.py benchmarks/results/three-cpu-TU_EJECUCION
```

La receta construye la imagen de aplicación actual y permite repetir el procedimiento desde el repositorio; no garantiza las mismas cifras. Conserva el script k6 y el análisis de finalización originales. Se verificaron la ayuda, la compilación y el análisis sobre artefactos históricos, **sin volver a ejecutar la carga** al preparar esta documentación. La lectura de contadores de CPU es opcional mediante `--resource-counters` y requiere Linux, Docker local y cgroups v2; sus requisitos se explican en la receta.

Para una comprobación breve del runner, sin repetir carga alta, puede usarse `python3 benchmarks/run.py --smoke`. Su [documentación](../benchmarks/README.md) explica preparación, recibos, integridad, descartes y códigos de salida.
