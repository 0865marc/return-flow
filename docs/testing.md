# Testing: proteger el comportamiento al cambiar el transporte

El objetivo de las pruebas es poder cambiar cómo entra y se ejecuta una operación
sin cambiar sus reglas, duplicar sus efectos ni confirmar trabajo que no se ha
guardado. La misma devolución puede llegar por HTTP directo, por un formato de
transportista o por un comando RabbitMQ: la identidad y el resultado deben seguir
siendo coherentes.

La suite de API contiene **286 casos parametrizados**: **196 sin servicios
externos y 90 de integración**. Hay además **45 pruebas del benchmark**, para un
total de **331 casos**. Son recuentos de pruebas, no un porcentaje de cobertura.
Los tests marcados `e2e` son un subconjunto de integración, no una suite adicional
que deba sumarse otra vez.

## Qué riesgo protege cada capa

| Capa y pruebas | Comportamiento que se comprueba | Riesgo que detecta |
| --- | --- | --- |
| [Dominio](../api/tests/domain/) | Transiciones válidas, devolución solo de una entrega entregada, validación al construir y modificar entidades. | Aceptar estados imposibles o alterar una entidad tras una validación fallida. |
| [Casos de uso](../api/tests/application/) | Orquestación con repositorios en memoria, ausencia de escrituras ante un rechazo y propagación de errores de infraestructura. | Duplicar reglas en adaptadores o confundir una caída de persistencia con un rechazo de negocio. |
| [HTTP y contratos de transportistas](../api/tests/adapters/http/) | Validación, códigos HTTP, claves de idempotencia, normalización Correos/DHL y uso del mismo puerto. La aceptación asíncrona no accede al almacén de comandos. | Cambiar el contrato público, mezclar formatos con reglas de negocio o volver a introducir SQL en el camino de aceptación. |
| [Persistencia PostgreSQL](../api/tests/adapters/persistence/) | Reutilización y cierre del pool, agotamiento y recuperación, escrituras obsoletas, transacciones, savepoints y comandos concurrentes. | Fugas de conexiones, resultados confirmados sin sus efectos o duplicación al competir dos ejecuciones. |
| [Publicador, consumidor y worker](../api/tests/adapters/messaging/) | Confirmaciones individuales, límite de publicaciones concurrentes, plazos, cancelación, cierre de recursos y orden entre commit y ACK. | Responder `202` sin confirmación, cerrar una conexión que usan otras publicaciones o reconocer un mensaje antes de guardar su resultado. |
| [Integración RabbitMQ](../api/tests/adapters/messaging/test_rabbitmq.py) | Trabajo sin consumidor, publicación sin destino, reentrega después de commit y antes de ACK, mensajes inválidos y cola de fallidos. | Perder o duplicar trabajo al reintentar y alterar el resultado original ante una clave incompatible. |
| [Flujos completos](../api/tests/e2e/) y [flujo asíncrono](../api/tests/adapters/messaging/test_rabbitmq.py) | Crear, entregar, solicitar y consultar una devolución con la aplicación real, persistencia tras recrear la aplicación y reintentos entre transportes. | Que piezas correctas por separado fallen al conectarse o no conserven su identidad al cambiar de entrada. |
| [Herramienta de benchmark](../benchmarks/tests/) | Planificación, límites del generador, ausencia de GET y reintentos, parada antes del snapshot, reconciliación e integridad. | Medir un escenario distinto del anunciado o presentar una aceptación como una operación terminada. |

Los fakes de repositorio permiten probar las reglas sin red. No sustituyen las
pruebas con PostgreSQL: el bloqueo concurrente, el rollback y la recuperación de
una conexión necesitan comprobarse con el adaptador real. Del mismo modo, un
doble AMQP sirve para provocar cancelaciones o confirmaciones negativas de forma
determinista, mientras que las pruebas con RabbitMQ comprueban el enrutamiento,
la reentrega y el dead-lettering reales.

## Casos decisivos para las dos variantes

La [idempotencia transaccional](../api/tests/adapters/persistence/test_commands.py)
tiene pruebas con varias ejecuciones simultáneas del mismo comando. Solo una
realiza el negocio; las demás reutilizan su resultado. También se comprueba que
reutilizar una clave con otro contenido falla y que un error revierte las
escrituras parciales antes de persistir el resultado fallido.

El [flujo directo](../api/tests/e2e/test_direct_idempotency.py) ejercita reintentos
concurrentes entre dos instancias de la aplicación y fallos simulados antes del
commit o después de guardarlo, antes de devolver la respuesta. Esto protege el
caso en que el cliente desconoce si debe repetir una petición.

En RabbitMQ, la prueba de [reentrega](../api/tests/adapters/messaging/test_rabbitmq.py)
cierra el canal después del commit y antes del ACK. El siguiente consumidor
recupera el resultado guardado sin crear otra devolución. Una prueba distinta
verifica que dos publicaciones concurrentes con el mismo `command_id` reciben
cada una su propia confirmación: compartir identidad de negocio no permite
confundir confirmaciones de mensajes.

Estas garantías se limitan a los efectos en PostgreSQL de un comando. Las pruebas
no demuestran ejecución global exactamente una vez, alta disponibilidad del
broker ni atomicidad con una llamada a un servicio externo.

## Ejecutar las pruebas

Los siguientes comandos se ejecutan desde la raíz del repositorio. Docker Compose
instala las dependencias en la imagen de API; no hace falta configurar Python en
el host para ejecutar sus tests.

Pruebas sin PostgreSQL ni RabbitMQ como dependencias de los casos:

```bash
docker compose build api
docker compose run --rm --no-deps api python -m pytest -m 'not integration'
```

Integración PostgreSQL y flujos completos directos:

```bash
docker compose up -d --build api
docker compose exec api sh -c 'TEST_DATABASE_URL="$DATABASE_URL" python -m pytest -m integration'
```

Los casos que necesitan RabbitMQ se omiten mientras no se defina
`TEST_RABBITMQ_URL`. Para ejecutar los **286 casos de API**, incluidos los dos
servicios reales:

```bash
docker compose up -d --build api rabbitmq
docker compose exec api sh -c 'TEST_DATABASE_URL="$DATABASE_URL" TEST_RABBITMQ_URL="$RABBITMQ_URL" python -m pytest'
```

Para seleccionar solo los flujos marcados `e2e` con ambos servicios disponibles:

```bash
docker compose exec api sh -c 'TEST_DATABASE_URL="$DATABASE_URL" TEST_RABBITMQ_URL="$RABBITMQ_URL" python -m pytest -m e2e'
```

No es necesario arrancar el servicio `worker` para los tests: las pruebas
asíncronas crean y detienen su propio consumidor o worker sobre colas exclusivas.
El backend de AnyIO utilizado por la suite es `asyncio`.

Las pruebas de la herramienta de carga solo necesitan Python 3 en el host:

```bash
python3 -m unittest discover -s benchmarks/tests -v
```

No arrancan Docker ni infraestructura de negocio. Algunas levantan un servidor
HTTP falso en localhost, por lo que el entorno debe permitir sockets locales.

Si ya se dispone de un entorno Python con las dependencias de
[`api/requirements.txt`](../api/requirements.txt), también se puede ejecutar
`python -m pytest` desde `api/`. Las variables `TEST_DATABASE_URL` y
`TEST_RABBITMQ_URL` controlan la integración. Sin ellas se omiten los casos que
requieren cada servicio; un resultado con tests omitidos no verifica esa capa.

## Aislamiento y alcance

La [fixture PostgreSQL](../api/tests/conftest.py) crea un esquema
`test_return_flow_<uuid>` por prueba, configura su `search_path` y elimina ese
esquema al terminar. La cuenta de pruebas debe poder crear y eliminar esquemas.
Las pruebas no truncan ni borran las tablas de la aplicación. La
[fixture RabbitMQ](../api/tests/adapters/messaging/test_rabbitmq.py) crea una cola
de trabajo y otra de fallidos con nombres únicos; al terminar elimina solo esas
colas y no purga las de desarrollo.

Los tests `e2e` recorren la aplicación mediante `TestClient`, con PostgreSQL y,
cuando corresponde, RabbitMQ reales. No atraviesan un proxy, una conexión HTTP
externa ni un despliegue multinodo. Los formatos de Correos y DHL son simulados:
se prueba nuestra normalización, no la compatibilidad con APIs de esos
transportistas.

El benchmark complementa estas pruebas con carga HTTP sobre contenedores.
Comprobar que una operación funciona y medir hasta qué carga se mantiene son
preguntas distintas. Su [metodología](../benchmarks/README.md) separa el envío de
las consultas de auditoría, distingue las peticiones no enviadas de los errores
del backend y comprueba las entidades además del estado de los comandos.

Como evolución del caso de estudio, serían útiles pruebas de migración de esquema
con datos representativos y compatibilidad entre versiones de mensajes durante
un despliegue gradual. Son trabajo pendiente; la suite actual no demuestra esas
propiedades.
