# Return-Flow

Simulación práctica de un sistema de entregas y devoluciones con Python y FastAPI,
PostgreSQL y RabbitMQ. El proyecto utiliza arquitectura hexagonal y acceso
asíncrono a PostgreSQL, con un entorno de desarrollo local basado en Docker Compose.
El mismo flujo puede ejecutarse dentro de la petición HTTP o mediante comandos
RabbitMQ procesados en un worker separado. Ambas variantes utilizan un pool de
conexiones y el mismo mecanismo de idempotencia transaccional en PostgreSQL.

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
Para trabajar solo con el flujo directo basta con `docker compose up -d --build api`:
Compose arranca también PostgreSQL y no necesita iniciar RabbitMQ ni el worker.

Para detener los servicios: `docker compose down`. Los datos se conservan en volúmenes; `docker compose down -v` también los elimina.

## Persistencia PostgreSQL

`adapters/persistence/postgres.py` es la única implementación de persistencia:
utiliza psycopg y reutiliza conexiones de `AsyncConnectionPool`. Los repositorios
de entregas, devoluciones y comandos comparten un pool por proceso; la API y el
worker tienen pools independientes. Cada pool se abre al arrancar y se cierra al
detener el proceso.

Las escrituras de negocio y el resultado del comando se confirman en una misma
transacción, tanto en las rutas directas como en el worker. Los repositorios
ejecutan el mismo SQL sobre la conexión ligada a esa transacción. Las consultas
obtienen una conexión del pool y la devuelven al terminar. El pool de la API usa
`autocommit=True`: una lectura aislada no necesita `BEGIN` y `COMMIT` adicionales.
`register()` y `execute()` siguen abriendo transacciones explícitas, de modo que
la escritura de negocio y el resultado permanecen atómicos. El pool del worker
conserva `autocommit=False`.

| Variable | Valor por defecto | Función |
|---|---|---|
| `DATABASE_POOL_MAX_SIZE` | `10` | Máximo de conexiones del pool por proceso. |
| `DATABASE_POOL_TIMEOUT` | `5` | Segundos de espera máxima para obtener una conexión. |

El pool mantiene al menos una conexión. Si todas están ocupadas, la operación
espera una disponible; si supera el tiempo de espera, la API responde `503`.
El acceso a PostgreSQL utiliza SQL directamente, sin ORM.

`open_repositories()` prepara los repositorios y libera el pool al terminar.
`composition.py` conecta siempre esta fábrica: no existe un selector de base de
datos ni la variable `PERSISTENCE_ADAPTER`. `main.py` registra el router HTTP y
gestiona los recursos durante el ciclo de vida de la aplicación. Los tests pueden
inyectar una fábrica explícita mediante `create_app(..., persistence_factory=...)`.

| Variante de ejecución | Persistencia | Respuesta HTTP |
|---|---|---|
| Directa (`postgres_pool` en el benchmark) | PostgreSQL con pool e idempotencia. | `201`/`200` después del commit de la operación. |
| RabbitMQ (`event_driven` en el benchmark) | El mismo adaptador e idempotencia, ejecutados en el worker. | `202` al aceptar el comando; el resultado se consulta después. |

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
- **Puertos:** definen las operaciones que ofrece la aplicación (entrada) o que
  necesita del exterior (salida), por ejemplo buscar o guardar una entrega.
- **Adaptadores:** conectan la aplicación con herramientas concretas. FastAPI
  recibe peticiones; PostgreSQL guarda datos; RabbitMQ transporta mensajes.
- **`main.py`:** arranca FastAPI y conecta los repositorios con las rutas.
- **`composition.py`:** conecta el único adaptador de persistencia y registra los
  adaptadores de transportistas; gestiona también el publicador opcional.
- **`worker.py`:** arranca el consumidor RabbitMQ con sus propios repositorios
  y ejecuta los casos de uso existentes desde el adaptador de mensajería.

Por ejemplo, una petición de escritura llega a FastAPI, que invoca el ejecutor
de comandos y este llama al caso de uso. El caso de uso aplica las reglas del
dominio y, si necesita guardar datos, utiliza un puerto. El adaptador PostgreSQL
implementa ese contrato y realiza la operación.

La regla de organización es que el dominio no necesita conocer HTTP, SQL ni
RabbitMQ, y los casos de uso conocen los contratos, no sus implementaciones.

`adapters/commands/contracts.py` define los comandos y sus resultados;
`executor.py` los ejecuta mediante los casos de uso existentes; `flow.py`
implementa el puerto `DeliveryFlow` con idempotencia para las rutas directas.
El consumidor RabbitMQ utiliza el mismo ejecutor. `adapters/messaging/` contiene
la configuración, las colas, el publicador y el consumidor.

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
    │   ├── dependencies.py
    │   └── integrations.py
    ├── carriers/
    │   ├── contracts.py
    │   ├── correos.py
    │   └── dhl.py
    ├── commands/
    │   ├── contracts.py
    │   ├── executor.py
    │   └── flow.py
    ├── persistence/
    └── messaging/
```

## Primer flujo: solicitar una devolución

El recorrido completo conecta FastAPI, los comandos, los casos de uso, el dominio
y PostgreSQL. Por ejemplo, `POST /returns` ejecuta un comando que llama a
`RequestReturn`: obtiene la entrega y guarda la devolución mediante los puertos
de repositorio. El resultado del comando se guarda junto con la devolución.

| Operación | Ruta directa | `Idempotency-Key` |
|---|---|---|
| Crear entrega | `POST /deliveries` | UUID obligatorio. |
| Marcarla como entregada | `POST /deliveries/{delivery_id}/deliver` | UUID opcional; recomendable para conservar el resultado del reintento. |
| Solicitar devolución | `POST /returns` | UUID obligatorio. |

Genera una clave por operación y conserva esa misma clave si necesitas
reintentarla. No compartas una clave entre la creación, la entrega y la devolución.

Puedes probarlo desde `http://localhost:8000/docs`, en este orden:

1. `POST /deliveries` con `Idempotency-Key`: crea una entrega pendiente. Copia el `id` de la respuesta.
2. `POST /deliveries/{delivery_id}/deliver`: marca esa entrega como entregada.
3. `POST /returns` con otra `Idempotency-Key`: solicita su devolución con este cuerpo:

   ```json
   {"delivery_id": "UUID_DE_LA_ENTREGA"}
   ```

4. `GET /returns/{return_id}`: consulta la devolución usando el `id` recibido.
   También puedes consultar la entrega con `GET /deliveries/{delivery_id}`.

Las creaciones devuelven `201` y las consultas y actualizaciones, `200`. La respuesta
de escritura espera al commit; no necesita RabbitMQ. Marcar de nuevo una entrega
ya entregada devuelve `200`. Un recurso inexistente devuelve `404`; una operación
no permitida por el negocio, `409`; y una entrada inválida o una clave obligatoria
ausente, `422`. Los datos permanecen disponibles al reiniciar la API.

El mismo flujo desde la terminal:

```bash
CREATE_KEY=$(python3 -c 'import uuid; print(uuid.uuid4())')
DELIVER_KEY=$(python3 -c 'import uuid; print(uuid.uuid4())')
RETURN_KEY=$(python3 -c 'import uuid; print(uuid.uuid4())')
DELIVERY_ID=$(curl -fsS -X POST http://localhost:8000/deliveries \
  -H "Idempotency-Key: $CREATE_KEY" \
  | python3 -c 'import json, sys; print(json.load(sys.stdin)["id"])')
curl -i -X POST "http://localhost:8000/deliveries/$DELIVERY_ID/deliver" \
  -H "Idempotency-Key: $DELIVER_KEY"
curl -i -X POST http://localhost:8000/returns \
  -H "Idempotency-Key: $RETURN_KEY" -H 'Content-Type: application/json' \
  -d "{\"delivery_id\":\"$DELIVERY_ID\"}"
curl "http://localhost:8000/commands/$RETURN_KEY"
```

`GET /commands/{command_id}` también permite consultar los resultados de las
operaciones directas, usando su `Idempotency-Key` como identificador.

## Simulación de transportistas: Correos y DHL

El router `adapters/http/integrations.py` expone tres rutas comunes a todos los
transportistas. La dependencia HTTP de `adapters/http/dependencies.py` lee la
cabecera obligatoria `X-Carrier: correos` o `X-Carrier: dhl` y selecciona el
adaptador del registro definido en `composition.py`.

Los adaptadores de `adapters/carriers/` validan y normalizan los datos del
transportista y traducen los resultados al formato correspondiente. No conocen
FastAPI, cabeceras ni códigos HTTP, y no ejecutan negocio. Esta traducción puede
reutilizarse desde otro transporte, como un consumidor de mensajes.

Después de validar y normalizar la entrada, el router llama al mismo puerto de
aplicación, `DeliveryFlow`. La dependencia `get_delivery_flow` proporciona
`IdempotentDeliveryFlow`, que convierte cada operación en un comando y utiliza
el ejecutor común y el almacén PostgreSQL. El ejecutor delega en `CreateDelivery`,
`MarkDeliveryDelivered` y `RequestReturn`. El dominio y los casos de uso permanecen
intactos; ni ellos ni el puerto conocen el transportista.

Para añadir otro formato, implementa `CarrierAdapter` en
`adapters/carriers/` y regístralo en `CARRIER_ADAPTERS` en `composition.py` antes
de arrancar la aplicación. Este protocolo define traducciones y modelos
Pydantic; el puerto de entrada al negocio sigue siendo `DeliveryFlow`. Las rutas
y los casos de uso permanecen iguales. El adaptador HTTP obtiene los contratos
del registro para documentarlos en OpenAPI.

Estos contratos son **ficticios y locales**: sirven para simular transportistas
que llaman a nuestra API; no reproducen interfaces oficiales de Correos o DHL.
La cabecera selecciona un formato simulado; no autentica al transportista. En
esta simulación no se llama a servicios de los transportistas ni se añade
latencia de red externa.

| Operación | Ruta compartida |
|---|---|
| Crear entrega | `POST /integrations/deliveries` |
| Marcar entregada | `POST /integrations/deliveries/{delivery_id}/deliver` |
| Solicitar devolución | `POST /integrations/returns` |

| Formato | Correos | DHL |
|---|---|---|
| Cabecera | `X-Carrier: correos` | `X-Carrier: dhl` |
| Cuerpo de devolución | `{"id_entrega": "UUID"}` | `{"shipmentId": "UUID"}` |
| Respuesta de entrega | `{"id_entrega": "UUID", "estado": "pendiente"}` | `{"shipmentId": "UUID", "status": "PENDING"}` |
| Respuesta de devolución | `{"id_devolucion": "UUID", "id_entrega": "UUID", "estado": "solicitada"}` | `{"returnId": "UUID", "shipmentId": "UUID", "status": "REQUESTED"}` |

Las creaciones requieren una cabecera `Idempotency-Key` con un UUID, además de
`X-Carrier`. Al marcar entregada, la clave es opcional. Las peticiones completan
el comando antes de responder: `201` al crear y `200` al marcar entregada.
Correos traduce los estados a `pendiente`, `entregada`,
`solicitada` y `completada`; DHL utiliza `PENDING`, `DELIVERED`, `REQUESTED` y
`COMPLETED`. Solicitar una devolución solo alcanza `requested`: todavía no existe
una operación HTTP para completarla físicamente.

Los identificadores son los UUID internos de nuestra aplicación, también válidos
en `GET /deliveries/{delivery_id}` y `GET /returns/{return_id}`. Esta versión no
mantiene referencias de seguimiento externas ni separa los recursos por
transportista. Ambos acceden al mismo modelo de entregas y devoluciones.

Con la API arrancada, puedes simular el flujo de Correos usando `curl` y Python 3:

```bash
CREATE_KEY=$(python3 -c 'import uuid; print(uuid.uuid4())')
DELIVER_KEY=$(python3 -c 'import uuid; print(uuid.uuid4())')
RETURN_KEY=$(python3 -c 'import uuid; print(uuid.uuid4())')
DELIVERY_ID=$(curl -fsS -X POST http://localhost:8000/integrations/deliveries \
  -H 'X-Carrier: correos' -H "Idempotency-Key: $CREATE_KEY" \
  | python3 -c 'import json, sys; print(json.load(sys.stdin)["id_entrega"])')
curl -i -X POST "http://localhost:8000/integrations/deliveries/$DELIVERY_ID/deliver" \
  -H 'X-Carrier: correos' -H "Idempotency-Key: $DELIVER_KEY"
curl -i -X POST http://localhost:8000/integrations/returns -H 'X-Carrier: correos' \
  -H "Idempotency-Key: $RETURN_KEY" -H 'Content-Type: application/json' \
  -d "{\"id_entrega\":\"$DELIVERY_ID\"}"
```

El mismo flujo con el contrato de DHL:

```bash
CREATE_KEY=$(python3 -c 'import uuid; print(uuid.uuid4())')
DELIVER_KEY=$(python3 -c 'import uuid; print(uuid.uuid4())')
RETURN_KEY=$(python3 -c 'import uuid; print(uuid.uuid4())')
DELIVERY_ID=$(curl -fsS -X POST http://localhost:8000/integrations/deliveries \
  -H 'X-Carrier: dhl' -H "Idempotency-Key: $CREATE_KEY" \
  | python3 -c 'import json, sys; print(json.load(sys.stdin)["shipmentId"])')
curl -i -X POST "http://localhost:8000/integrations/deliveries/$DELIVERY_ID/deliver" \
  -H 'X-Carrier: dhl' -H "Idempotency-Key: $DELIVER_KEY"
curl -i -X POST http://localhost:8000/integrations/returns -H 'X-Carrier: dhl' \
  -H "Idempotency-Key: $RETURN_KEY" -H 'Content-Type: application/json' \
  -d "{\"shipmentId\":\"$DELIVERY_ID\"}"
```

También aparecen en `/docs`, identificadas como simulaciones. Una entrega
inexistente devuelve `404` y una devolución antes de entregar devuelve `409`.
Una segunda confirmación de entrega devuelve `200`. La ausencia de `X-Carrier`,
un proveedor desconocido o un cuerpo incompatible con el formato seleccionado devuelven `422`,
igual que los UUID o campos inválidos y la ausencia de una clave obligatoria.
Estas rutas utilizan la misma idempotencia que las rutas genéricas.

## Idempotencia compartida

Cada clave identifica un comando en PostgreSQL. Repetir la misma clave y el
mismo contenido normalizado conserva el resultado y no crea otra entrega o
devolución. Reutilizarla para otro tipo de comando o para otros datos devuelve
`409`. Una clave distinta representa otra operación: dos claves distintas pueden
crear dos devoluciones de la misma entrega.

Las claves son globales dentro de la aplicación: las comparten las rutas directas,
las asíncronas y los formatos Correos/DHL. Por ejemplo, reintentar una devolución
directa mediante `/returns-async` con la misma clave y entrega recupera el mismo
comando. Cambiar de formato de transportista conserva el recurso si los datos
normalizados son los mismos; la respuesta se traduce al formato solicitado.

`CommandExecutor` utiliza `PostgresCommandStore` para bloquear el comando con
`SELECT ... FOR UPDATE` y ejecutar los casos de uso sobre una misma conexión.
Las escrituras de negocio y el resultado se confirman en una sola transacción.
Las ejecuciones concurrentes del mismo comando esperan el bloqueo y reutilizan
el resultado. Si falla la ejecución, un savepoint revierte sus escrituras y se
guarda un resultado `failed`; los fallos de conexión que impiden completar la
transacción se propagan para permitir el reintento.

Los rechazos de negocio también se conservan: las rutas directas devuelven el
mismo `404` o `409` al repetir un comando fallido. Un `error_code` interno permite
reconstruir la respuesta HTTP; los errores de ejecución no reconocidos devuelven
`500`. Los errores de validación HTTP se rechazan antes de ejecutar el comando.
Después de corregir la causa de un comando `failed`, se necesita una clave nueva
para solicitar una ejecución nueva.

La actualización añade `error_code` sin borrar registros anteriores. Los fallos
históricos sin ese código conservan su error, pero al repetirlos por HTTP directo
se devuelve `500`: no se deduce un `404` o `409` a partir del texto del mensaje.

La idempotencia protege cada comando, no convierte todo el flujo en una única
transacción. Tampoco hace atómicas posibles llamadas futuras a servicios externos.

## Flujo asíncrono mediante RabbitMQ

El consumidor de mensajería utiliza el mismo `CommandExecutor` que el flujo
directo, sin modificar el dominio ni los casos de uso. Los mensajes son comandos
versionados: solicitan una operación y conservan su `command_id` al reentregarse.
El modelo comienza con una entrega; no existe una entidad de pedido independiente.

| Operación | Ruta | `Idempotency-Key` |
|---|---|---|
| Crear entrega | `POST /deliveries-async` | UUID obligatorio. |
| Marcarla como entregada | `POST /deliveries/{delivery_id}/deliver-async` | UUID opcional por compatibilidad; recomendable para reintentos. |
| Solicitar devolución | `POST /returns-async` | UUID obligatorio. |
| Consultar estado y resultado | `GET /commands/{command_id}` | No necesita cabecera. |

La API registra primero el comando como `pending` en PostgreSQL y lo publica. El publicador reutiliza una conexión y permite varias
publicaciones pendientes de confirmación, con un límite configurable. Cada petición
espera su propia confirmación antes de responder. Un fallo o una cancelación no
cierra la conexión mientras otras publicaciones todavía la utilizan.
Cuando RabbitMQ confirma la publicación devuelve `202 Accepted`, con
`{"command_id": "UUID_DEL_COMANDO", "status": "accepted"}` y la cabecera
`Location: /commands/UUID_DEL_COMANDO`. El `202` confirma la aceptación; hay que
consultar el comando para saber si la operación terminó:

| Estado | Significado |
|---|---|
| `pending` | Todavía no hay un resultado confirmado; `result` y `error` son `null`. |
| `completed` | `result` contiene la entrega o devolución creada/actualizada, incluido su `id`. |
| `failed` | `error` describe el fallo; `result` es `null` y las escrituras de negocio se han revertido. |

PostgreSQL conserva `created_at` al registrar cada comando y `completed_at` al
persistir su resultado terminal, tanto correcto como fallido. Los reintentos
conservan esos tiempos. El resultado y la escritura de negocio siguen dentro de
la misma transacción; `completed_at` se toma justo antes del commit, no mide el
instante exacto del commit. La actualización del esquema mantiene `NULL` en los
tiempos históricos desconocidos de comandos anteriores.

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

La clave de idempotencia es el `command_id` y sigue las reglas compartidas por
ambas variantes. Si el comando sigue `pending`, repetir el POST asíncrono vuelve
a publicarlo; si terminó, devuelve `202` con el mismo identificador sin
republicarlo. Esto también se aplica a comandos ejecutados previamente por una
ruta directa. Un UUID inválido o la ausencia de la cabecera obligatoria devuelve
`422`. El comando `ensure_delivery_delivered`, utilizado por ambos flujos,
admite que la entrega ya esté entregada.

El worker utiliza la transacción idempotente descrita en el apartado anterior.
Los consumidores del mismo comando esperan el bloqueo y después reutilizan su
resultado, sin repetir el caso de uso.

La cola y su cola de fallidos son durables. El publicador envía mensajes
persistentes con *publisher confirms* y `mandatory`. Usa `basic_publish(wait=False)`
para evitar la espera intermedia de escritura local mientras el canal está
bloqueado, y sigue esperando la confirmación de RabbitMQ antes de responder `202`.
El worker procesa hasta ocho comandos concurrentes por defecto; `prefetch` tiene
el mismo valor que `RABBITMQ_WORKER_CONCURRENCY`. Cada mensaje recibe `ACK` solo
después de confirmar o recuperar un resultado `completed`; los comandos `failed`
y los mensajes inválidos se rechazan
sin reencolar y van a `<RABBITMQ_QUEUE>.failed`. Si se pierde la conexión a la base
de datos o no puede persistirse el resultado, el worker termina sin `ACK` y Compose
lo reinicia. RabbitMQ puede reentregar el mensaje al cerrarse el canal o conexión.
Una caída entre `COMMIT` y `ACK` reutiliza el resultado guardado.

La concurrencia permite aprovechar las esperas de entrada/salida con un único
proceso. Ocho es el valor inicial probado con un pool de diez conexiones; puede
ajustarse según la carga. Los comandos distintos pueden terminar en diferente
orden: el cliente debe seguir esperando la finalización de cada dependencia del
flujo. Al detenerse el worker, sus tareas se cancelan y terminan antes de cerrar
la conexión y el pool; los mensajes sin ACK pueden reentregarse.

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
| `RABBITMQ_PUBLISH_TIMEOUT` | `5` | Límite en segundos, incluida la espera de capacidad, conexión y confirmación. |
| `RABBITMQ_PUBLISH_CONCURRENCY` | `64` | Publicaciones pendientes de confirmación como máximo, por proceso de API. |
| `RABBITMQ_WORKER_CONCURRENCY` | `8` | Comandos simultáneos por worker; también fija el prefetch. |

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
`TEST_DATABASE_URL`. Las pruebas de persistencia comprueban la única implementación
PostgreSQL con pool. Las end-to-end utilizan la aplicación real mediante
`TestClient` y comprueban que la devolución sigue disponible al reiniciar la API.
También se comprueban la reutilización de conexiones, el rollback tras errores,
el agotamiento del pool y su cierre al detener la aplicación. Las pruebas del
flujo directo cubren creaciones repetidas con la misma clave, conflictos de
contenido, resultados fallidos conservados y las rutas de transportistas.
Las pruebas asíncronas utilizan el soporte de AnyIO con el backend `asyncio`.

Para incluir también la integración con RabbitMQ:

```bash
docker compose up -d --build api rabbitmq
docker compose exec api sh -c 'TEST_DATABASE_URL="$DATABASE_URL" TEST_RABBITMQ_URL="$RABBITMQ_URL" python -m pytest'
```

Las pruebas RabbitMQ se habilitan con `TEST_RABBITMQ_URL`; las que comprueban
persistencia necesitan también `TEST_DATABASE_URL`. Crean colas con nombres
únicos y las eliminan al terminar, sin consumir ni purgar las colas de la
aplicación. Comprueban el flujo asíncrono completo con PostgreSQL y pool,
los reintentos con la misma clave, los conflictos de contenido y
los fallos de negocio consultables y enviados a la cola de fallidos. También
cubren la espera sin consumidor, la reentrega después del commit y antes del
`ACK` sin duplicar una devolución, los mensajes inválidos y la publicación sin
destino válido. Las pruebas de persistencia verifican la serialización de
comandos concurrentes y la atomicidad de las escrituras; las unitarias cubren
los errores del consumidor y de HTTP. Los contratos y el ejecutor de comandos
son compartidos por el procesamiento directo y el worker.

## Evaluación de rendimiento y fiabilidad

El evaluador de `benchmarks/` compara `postgres_pool` y `event_driven`.
La primera completa el trabajo dentro de la petición HTTP; la segunda usa
RabbitMQ y espera el resultado del worker. Ambas utilizan PostgreSQL con pool,
código Python asíncrono, los mismos casos de uso y el mismo mecanismo de
idempotencia. La versión sin pool se ha eliminado. Un script de Python arranca un
entorno Docker aislado, ejecuta carga HTTP con k6, comprueba los datos persistidos
y guarda los resultados en archivos locales. No requiere Grafana ni utiliza los
datos de desarrollo.

Para comprobar que funciona, desde la raíz del proyecto:

```bash
python3 benchmarks/run.py --smoke
```

Para comparar las dos opciones con 2 CPU de aplicación:

```bash
python3 benchmarks/run.py --adapters postgres_pool event_driven --app-cpus 2
```

Por defecto, compara el flujo completo a 10, 50 y 400 flujos por segundo, con
tres repeticiones por variante y carga: 18 mediciones en total. La variante
directa asigna 2 CPU a la API con dos procesos; eventos reparte 1 CPU para la
API y 1 CPU para el worker. PostgreSQL tiene 1 CPU en ambas variantes y
RabbitMQ añade otra CPU en eventos, fuera del presupuesto de aplicación.

La prueba corta valida el evaluador; no sirve para sacar conclusiones de
rendimiento. Los resultados incluyen `execution_model: "async"` y distinguen
`processing_model: "direct"` o `"queued"`; los
resultados anteriores conservan su significado y no deben mezclarse solo porque
coincidan los nombres de los adaptadores.

La medición continúa aunque el calentamiento incumpla los umbrales de carga,
para observar también qué ocurre bajo saturación. El informe separa peticiones
HTTP fallidas, flujos incompletos y operaciones que k6 no llegó a iniciar; además,
audita las escrituras confirmadas en PostgreSQL. Una petición fallida no demuestra
por sí sola que se haya perdido una escritura. En eventos, el `202` no cuenta
como operación terminada. Se consultan solo los dos comandos necesarios para
avanzar entre pasos, con esperas crecientes; la devolución final se verifica en
PostgreSQL después de la carga. La variante directa ejecuta tres POST sin
GET de comprobación. En eventos se permite al worker terminar el trabajo pendiente
hasta un plazo configurable antes de detener los procesos y auditar.

Cada intento tiene un recibo con identificador y tiempos. Solo en el PostgreSQL
del benchmark se activa `track_commit_timestamp`, igual para las dos variantes:
la latencia del flujo termina en el commit de la devolución. La auditoría reconstruye
flujos completos, sus latencias y los comandos pendientes, fallidos o ausentes;
no interpreta un timeout como pérdida definitiva. El formato de resultados es
`5`: ambas variantes envían claves de idempotencia para las tres operaciones y
persisten sus resultados como comandos. Los resultados históricos del formato
`4` no son directamente comparables: el flujo directo no tenía ese trabajo de
idempotencia. Las versiones anteriores también utilizaban otra metodología de
polling. Es necesario ejecutar una medición nueva para comparar las dos
implementaciones actuales.

Consulta la [metodología y los comandos de evaluación](benchmarks/README.md)
para configurar la carga, las repeticiones y leer sus resultados.
