# Return-Flow

Simulación práctica de un sistema de entregas y devoluciones con Python y FastAPI,
PostgreSQL y RabbitMQ. El proyecto utiliza arquitectura hexagonal y acceso
asíncrono a PostgreSQL, con un entorno de desarrollo local basado en Docker Compose.
RabbitMQ permite crear entregas, marcarlas como entregadas y solicitar devoluciones
mediante comandos procesados en un worker separado.

## Desarrollo local

Requiere Docker con Docker Compose. Para arrancar FastAPI, el worker, PostgreSQL y RabbitMQ:

```bash
docker compose up -d --build
```

Conexiones desde la máquina local (usuario y contraseña de desarrollo: `return_flow`):

- PostgreSQL: `postgresql://return_flow:return_flow@localhost:5432/return_flow`
- RabbitMQ: `amqp://return_flow:return_flow@localhost:5672/`

La API está disponible en `http://localhost:8000`, con documentación interactiva en
`http://localhost:8000/docs` y un endpoint `GET /health` que devuelve `{"status": "ok"}`.
El código, el Dockerfile y las dependencias de FastAPI están en `api/`.
Los cambios en el código de `api/` recargan automáticamente el servidor.
El worker no tiene recarga automática: después de cambiar su código, ejecuta
`docker compose restart worker`. Los cambios de dependencias requieren reconstruir
los servicios con `docker compose up -d --build`.

El servicio `api` usa `DATABASE_URL` para conectar con PostgreSQL. Compose espera
a que la base de datos esté disponible y la API crea las tablas que falten al
arrancar. Los datos existentes se conservan. `/health` comprueba que la API responde.
La inicialización de tablas utiliza un bloqueo transaccional para que la API y
el worker puedan arrancar simultáneamente sobre una base vacía.

La API conecta con RabbitMQ al publicar el primer comando, por lo que las rutas
síncronas siguen disponibles si el broker está caído. El worker espera a que
PostgreSQL y RabbitMQ estén disponibles antes de arrancar.

Para detener los servicios: `docker compose down`. Los datos se conservan en volúmenes; `docker compose down -v` también los elimina.

## Persistencia PostgreSQL

La API utiliza por defecto `adapters/persistence/postgres_pool.py`: un adaptador
asíncrono con psycopg que reutiliza conexiones. Los repositorios de entregas y
devoluciones comparten un pool por proceso; la API y el worker tienen recursos
independientes. El pool se abre al arrancar y se
cierra al detener la aplicación. En las rutas síncronas, cada operación termina
su transacción y devuelve la conexión al pool. El worker mantiene una misma
conexión y transacción para las escrituras de negocio y el resultado del comando.

La alternativa, `adapters/persistence/postgres.py`, abre una conexión asíncrona
nueva por operación y la cierra al terminar. Ambos adaptadores cumplen los mismos
puertos y utilizan las mismas rutas y casos de uso con `async/await`.

```bash
PERSISTENCE_ADAPTER=postgres_pool docker compose up -d --build api worker
PERSISTENCE_ADAPTER=postgres docker compose up -d --build api worker
```

Ejecuta uno de los comandos según la variante que quieras utilizar.

| Variable | Valor por defecto | Función |
|---|---|---|
| `PERSISTENCE_ADAPTER` | `postgres_pool` | Selecciona `postgres` o `postgres_pool`. |
| `DATABASE_POOL_MAX_SIZE` | `10` | Máximo de conexiones del pool por proceso. |
| `DATABASE_POOL_TIMEOUT` | `5` | Segundos de espera máxima para obtener una conexión. |

El pool mantiene al menos una conexión. Si todas están ocupadas, la operación
espera una disponible; si supera el tiempo de espera, la API responde `503`.
Las variables del pool se aplican solo a `postgres_pool`.
Las dos variantes ejecutan el mismo SQL directamente, sin ORM, y responden
después de persistir la operación.

Cada adaptador expone `open_repositories()`, que prepara sus repositorios y libera
sus recursos al terminar. Al crear la aplicación, `composition.py` selecciona
la fábrica en `PERSISTENCE_FACTORIES`. `main.py` registra el único router HTTP
y gestiona el ciclo de vida del adaptador seleccionado.

| Variante | Conexiones PostgreSQL |
|---|---|
| `postgres` | Abre y cierra una conexión asíncrona por operación. |
| `postgres_pool` | Obtiene y devuelve una conexión de `AsyncConnectionPool`. |

Las rutas síncronas esperan a los casos de uso y estos esperan a los repositorios mediante
`await`. El dominio conserva métodos normales: aplicar una regla de negocio no
necesita esperar a la red. `async/await` permite atender otras peticiones mientras
PostgreSQL responde; la respuesta HTTP sigue confirmando una operación terminada.

## Arquitectura de la API

La arquitectura hexagonal separa las reglas del negocio de las herramientas que
usamos para recibir peticiones, guardar datos o enviar mensajes. El objetivo es
poder cambiar esas herramientas y probar el negocio de forma independiente.

Cada parte tiene una responsabilidad:

- **Dominio:** define los conceptos del negocio y las reglas que deben cumplir.
- **Casos de uso:** coordinan los pasos de una operación, como solicitar una
  devolución.
- **Puertos:** son contratos que indican qué operaciones necesita la aplicación
  del exterior, por ejemplo buscar o guardar una entrega.
- **Adaptadores:** conectan la aplicación con herramientas concretas. FastAPI
  recibe peticiones; PostgreSQL guarda datos; RabbitMQ transporta mensajes.
- **`main.py`:** arranca FastAPI y conecta los repositorios con las rutas.
- **`composition.py`:** selecciona los repositorios de la variante elegida;
  cada adaptador de persistencia gestiona su inicialización y cierre.
- **`worker.py`:** arranca el consumidor RabbitMQ con sus propios repositorios
  y ejecuta los casos de uso existentes desde el adaptador de mensajería.

Por ejemplo, una petición llega a FastAPI, que llama a un caso de uso. Este aplica
las reglas del dominio y, si necesita guardar datos, utiliza un puerto. El
adaptador de PostgreSQL implementa ese contrato y realiza la operación.

La regla de organización es que el dominio no necesita conocer HTTP, SQL ni
RabbitMQ, y los casos de uso conocen los contratos, no sus implementaciones.

Esta es la estructura de la API. `adapters/messaging/` contiene el contrato del
comando, la configuración, las colas, el publicador y el consumidor.

```text
api/
├── main.py
├── worker.py
├── composition.py
├── domain/
├── application/
│   ├── use_cases/
│   └── ports/
└── adapters/
    ├── http/
    ├── persistence/
    └── messaging/
```

## Primer flujo: solicitar una devolución

El recorrido completo conecta FastAPI, los casos de uso, los dominios y PostgreSQL.
Por ejemplo, `POST /returns` llama a `RequestReturn`, que obtiene la entrega y guarda
la devolución mediante los puertos de repositorio. Los adaptadores de PostgreSQL
implementan esos puertos.

Puedes probarlo desde `http://localhost:8000/docs`, en este orden:

1. `POST /deliveries`: crea una entrega pendiente. Copia el `id` de la respuesta.
2. `POST /deliveries/{delivery_id}/deliver`: marca esa entrega como entregada.
3. `POST /returns`: solicita su devolución con este cuerpo:

   ```json
   {"delivery_id": "UUID_DE_LA_ENTREGA"}
   ```

4. `GET /returns/{return_id}`: consulta la devolución usando el `id` recibido.
   También puedes consultar la entrega con `GET /deliveries/{delivery_id}`.

Las creaciones devuelven `201` y las consultas y actualizaciones, `200`. Un recurso
inexistente devuelve `404`; una operación no permitida por el negocio, `409`; y
una entrada inválida, `422`. Los datos permanecen disponibles al reiniciar la API.

## Flujo asíncrono mediante RabbitMQ

El adaptador de mensajería ejecuta `CreateDelivery`, `MarkDeliveryDelivered` y
`RequestReturn` sin modificar el dominio ni los casos de uso. Los mensajes son
comandos versionados: solicitan una operación y conservan su `command_id` al
reentregarse. El modelo comienza con una entrega; no existe una entidad de pedido
independiente.

| Operación | Ruta | `Idempotency-Key` |
|---|---|---|
| Crear entrega | `POST /deliveries-async` | UUID obligatorio. |
| Marcarla como entregada | `POST /deliveries/{delivery_id}/deliver-async` | UUID opcional por compatibilidad; recomendable para reintentos. |
| Solicitar devolución | `POST /returns-async` | UUID obligatorio. |
| Consultar estado y resultado | `GET /commands/{command_id}` | No necesita cabecera. |

La API registra primero el comando como `pending` en PostgreSQL y lo publica.
Cuando RabbitMQ confirma la publicación devuelve `202 Accepted`, con
`{"command_id": "UUID_DEL_COMANDO", "status": "accepted"}` y la cabecera
`Location: /commands/UUID_DEL_COMANDO`. El `202` confirma la aceptación; hay que
consultar el comando para saber si la operación terminó:

| Estado | Significado |
|---|---|
| `pending` | Todavía no hay un resultado confirmado; `result` y `error` son `null`. |
| `completed` | `result` contiene la entrega o devolución creada/actualizada, incluido su `id`. |
| `failed` | `error` describe el fallo; `result` es `null` y las escrituras de negocio se han revertido. |

`GET /commands/{command_id}` devuelve también `command_id` y `type`. Un comando
desconocido devuelve `404`. Los estados `completed` y `failed` son definitivos.
Las reglas de negocio se comprueban en el worker: una entrega inexistente o
todavía pendiente puede producir `202` y después un comando `failed`.

Para probar el flujo, genera una clave por operación y consérvala para sus
reintentos. Este ejemplo usa `curl` y Python 3:

```bash
CREATE_KEY=$(python3 -c 'import uuid; print(uuid.uuid4())')
DELIVER_KEY=$(python3 -c 'import uuid; print(uuid.uuid4())')
RETURN_KEY=$(python3 -c 'import uuid; print(uuid.uuid4())')
curl -i -X POST http://localhost:8000/deliveries-async -H "Idempotency-Key: $CREATE_KEY"
curl "http://localhost:8000/commands/$CREATE_KEY"
```

Repite el último `GET` hasta obtener `completed` y copia `result.id` como
`DELIVERY_ID`. Si aparece `failed`, revisa `error` antes de continuar.

```bash
DELIVERY_ID=UUID_DE_LA_ENTREGA
curl -i -X POST "http://localhost:8000/deliveries/$DELIVERY_ID/deliver-async" -H "Idempotency-Key: $DELIVER_KEY"
curl "http://localhost:8000/commands/$DELIVER_KEY"
```

Espera a `completed`, con `result.status` igual a `delivered`, antes de solicitar
la devolución:

```bash
curl -i -X POST http://localhost:8000/returns-async \
  -H "Idempotency-Key: $RETURN_KEY" -H 'Content-Type: application/json' \
  -d "{\"delivery_id\":\"$DELIVERY_ID\"}"
curl "http://localhost:8000/commands/$RETURN_KEY"
```

Cuando el último comando esté `completed`, `result.id` permite consultar
`GET /returns/{return_id}`. El resultado incluye también `delivery_id` y
`status: requested`.

La clave de idempotencia es el `command_id`: repetir el mismo `POST` con la misma
clave y contenido conserva el resultado. Si sigue `pending`, vuelve a publicar
el comando; si terminó, devuelve el mismo identificador sin republicarlo. Reusar
la clave con otro tipo de operación o contenido devuelve `409`. Un UUID inválido,
o la ausencia de la cabecera obligatoria, devuelve `422`. Una clave distinta
representa otra operación y conserva el comportamiento actual del negocio: dos
comandos distintos pueden crear dos devoluciones de una misma entrega. La ruta
síncrona `/deliver` sigue devolviendo `409` si ya estaba entregada; el comando
`ensure_delivery_delivered` admite que ese estado ya se haya alcanzado.

El worker bloquea el registro del comando con `SELECT ... FOR UPDATE` y ejecuta
el caso de uso utilizando repositorios que comparten su conexión. Las escrituras
de negocio y `completed` con su resultado se confirman en una sola transacción.
Un fallo de ejecución revierte las escrituras parciales mediante un savepoint y
guarda `failed`. Los consumidores del mismo comando esperan el bloqueo y después
reutilizan su resultado, sin repetir el caso de uso.

La cola y su cola de fallidos son durables. El publicador envía mensajes
persistentes con *publisher confirms* y `mandatory`. El worker procesa un mensaje
a la vez (`prefetch=1`): hace `ACK` solo después de confirmar o recuperar un
resultado `completed`; los comandos `failed` y los mensajes inválidos se rechazan
sin reencolar y van a `<RABBITMQ_QUEUE>.failed`. Si se pierde la conexión a la base
de datos o no puede persistirse el resultado, el worker termina sin `ACK` y Compose
lo reinicia. RabbitMQ puede reentregar el mensaje al cerrarse el canal o conexión.
Una caída entre `COMMIT` y `ACK` reutiliza el resultado guardado.

Esta versión no tiene outbox. Si falla la publicación después del registro, la
API devuelve `503` con `command_id` y `Location`; el mensaje podría haber llegado
al broker y el comando puede seguir `pending`. Repite el mismo `POST`, cuerpo y
`Idempotency-Key` cuando el servicio vuelva a estar disponible. No generes otra
clave para ese reintento: podría crear otro recurso. No hay un proceso que
republique automáticamente los comandos pendientes que no llegaron a la cola.

| Variable | Valor por defecto | Función |
|---|---|---|
| `RABBITMQ_URL` | URL del servicio en Compose | Conexión AMQP; obligatoria para el worker y para publicar. |
| `RABBITMQ_QUEUE` | `return_flow.deliveries.commands` | Cola de trabajo; la cola de fallidos añade `.failed`. |
| `RABBITMQ_CONNECT_TIMEOUT` | `5` | Límite en segundos para conectar. |
| `RABBITMQ_PUBLISH_TIMEOUT` | `5` | Límite en segundos de la operación de publicación, incluida la conexión. |

Para comprobar que el trabajo espera aunque el worker esté detenido:

```bash
docker compose stop worker
```

Envía `POST /deliveries-async` con una clave nueva desde `/docs`. La respuesta será
`202`, pero `GET /commands/{command_id}` seguirá mostrando `pending`. Después:

```bash
docker compose start worker
docker compose logs -f worker
```

El comando pasa a `completed`, con la entrega en `result`, y los logs muestran
su `command_id`.

No se eliminan automáticamente los registros de comandos ni se reintentan los
que terminaron en `failed`. Tras revisar y corregir la causa, una nueva solicitud
debe usar otra clave; republicar el comando fallido conserva su resultado.
Compose utiliza un único nodo RabbitMQ y colas clásicas: no ofrece alta
disponibilidad ni una garantía absoluta de entrega a la cola de fallidos si falla
el broker durante el dead-lettering.

## Testing

Las pruebas usan `pytest` y siguen la misma organización que el código de la API:

```text
api/tests/
├── domain/
├── application/
├── adapters/
└── e2e/
```

Las pruebas de dominio, casos de uso y HTTP con repositorios en memoria se ejecutan
sin una base de datos de pruebas. Desde la raíz del proyecto:

```bash
docker compose up -d --build api
docker compose exec api python -m pytest
```

Para ejecutar también persistencia y el flujo end-to-end contra PostgreSQL:

```bash
docker compose exec api sh -c 'TEST_DATABASE_URL="$DATABASE_URL" python -m pytest'
```

Estas pruebas crean un esquema temporal por prueba y lo eliminan al terminar,
sin modificar las tablas de la aplicación. Se omiten cuando no está definida
`TEST_DATABASE_URL`. Las pruebas de persistencia y end-to-end se ejecutan para
las dos variantes. Las end-to-end utilizan la aplicación real mediante
`TestClient` y comprueban que la devolución sigue disponible al reiniciar la API.
También se comprueban la reutilización de conexiones, el rollback tras errores,
el agotamiento del pool y su cierre al detener la aplicación.
Las pruebas asíncronas utilizan el soporte de AnyIO con el backend `asyncio`.

Para incluir también la integración con RabbitMQ:

```bash
docker compose up -d --build api rabbitmq
docker compose exec api sh -c 'TEST_DATABASE_URL="$DATABASE_URL" TEST_RABBITMQ_URL="$RABBITMQ_URL" python -m pytest'
```

Las pruebas RabbitMQ se habilitan con `TEST_RABBITMQ_URL`; las que comprueban
persistencia necesitan también `TEST_DATABASE_URL`. Crean colas con nombres
únicos y las eliminan al terminar, sin consumir ni purgar las colas de la
aplicación. Comprueban el flujo asíncrono completo con ambas variantes de
persistencia, los reintentos con la misma clave, los conflictos de contenido y
los fallos de negocio consultables y enviados a la cola de fallidos. También
cubren la espera sin consumidor, la reentrega después del commit y antes del
`ACK` sin duplicar una devolución, los mensajes inválidos y la publicación sin
destino válido. Las pruebas de persistencia verifican la serialización de
comandos concurrentes y la atomicidad de las escrituras; las unitarias cubren
los errores del consumidor y de HTTP.

## Evaluación de rendimiento y fiabilidad

El evaluador de `benchmarks/` compara `postgres` y `postgres_pool`, ambos asíncronos,
con la misma API. Un script de Python arranca un entorno Docker aislado, ejecuta
carga HTTP con k6, comprueba los datos persistidos y guarda los resultados en
archivos locales. No requiere Grafana ni utiliza los datos de desarrollo.

Para comprobar que funciona, desde la raíz del proyecto:

```bash
python3 benchmarks/run.py --smoke
```

Para comparar una conexión nueva por operación frente a reutilizar un pool:

```bash
python3 benchmarks/run.py --adapters postgres postgres_pool
```

Por defecto, compara el flujo completo a 10, 50 y 400 flujos por segundo, con
tres repeticiones por adaptador y carga: 18 mediciones en total.

La prueba corta valida el evaluador; no sirve para sacar conclusiones de
rendimiento. Los nuevos resultados incluyen `execution_model: "async"`; los
resultados anteriores conservan su significado y no deben mezclarse solo porque
coincidan los nombres de los adaptadores.

La medición continúa aunque el calentamiento incumpla los umbrales de carga,
para observar también qué ocurre bajo saturación. El informe separa peticiones
HTTP fallidas, flujos incompletos y operaciones que k6 no llegó a iniciar; además,
audita las escrituras confirmadas en PostgreSQL. Una petición fallida no demuestra
por sí sola que se haya perdido una escritura. Si es necesario reiniciar la API
para asegurar que no quedan operaciones pendientes, se registra en el resultado.

Consulta la [metodología y los comandos de evaluación](benchmarks/README.md)
para configurar escenarios, carga y repeticiones, y leer sus resultados.
