# Return-Flow

Simulación práctica de un sistema de entregas y devoluciones con Python y FastAPI,
PostgreSQL y RabbitMQ. El proyecto utiliza arquitectura hexagonal y acceso
asíncrono a PostgreSQL, con un entorno de desarrollo local basado en Docker Compose.
La integración de eventos con RabbitMQ queda para un experimento posterior.

## Desarrollo local

Requiere Docker con Docker Compose. Para arrancar FastAPI, PostgreSQL y RabbitMQ:

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

El servicio `api` usa `DATABASE_URL` para conectar con PostgreSQL. Compose espera
a que la base de datos esté disponible y la API crea las tablas que falten al
arrancar. Los datos existentes se conservan. `/health` comprueba que la API responde.

El primer flujo utiliza HTTP y PostgreSQL. RabbitMQ queda preparado para incorporar
eventos en una siguiente iteración.

Para detener los servicios: `docker compose down`. Los datos se conservan en volúmenes; `docker compose down -v` también los elimina.

## Persistencia PostgreSQL

La API utiliza por defecto `adapters/persistence/postgres_pool.py`: un adaptador
asíncrono con psycopg que reutiliza conexiones. Los repositorios de entregas y
devoluciones comparten un pool por proceso de la API. Se abre al arrancar y se
cierra al detener la aplicación; cada operación termina su transacción y devuelve
la conexión al pool.

La alternativa, `adapters/persistence/postgres.py`, abre una conexión asíncrona
nueva por operación y la cierra al terminar. Ambos adaptadores cumplen los mismos
puertos y utilizan las mismas rutas y casos de uso con `async/await`.

```bash
PERSISTENCE_ADAPTER=postgres_pool docker compose up -d --build api
PERSISTENCE_ADAPTER=postgres docker compose up -d --build api
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

Las rutas esperan a los casos de uso y estos esperan a los repositorios mediante
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

Por ejemplo, una petición llega a FastAPI, que llama a un caso de uso. Este aplica
las reglas del dominio y, si necesita guardar datos, utiliza un puerto. El
adaptador de PostgreSQL implementa ese contrato y realiza la operación.

La regla de organización es que el dominio no necesita conocer HTTP, SQL ni
RabbitMQ, y los casos de uso conocen los contratos, no sus implementaciones.

Esta es la estructura de la API. `adapters/messaging/` queda pendiente para la
integración de eventos.

```text
api/
├── main.py
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

## Evaluación de rendimiento y fiabilidad

El evaluador de `benchmarks/` compara `postgres` y `postgres_pool`, ambos asíncronos,
con la misma API. Un script de Python arranca un entorno Docker aislado, ejecuta
carga HTTP con k6, comprueba los datos persistidos y guarda los resultados en
archivos locales. No requiere Grafana ni utiliza los datos de desarrollo.

Para comprobar que funciona, desde la raíz del proyecto:

```bash
python3 benchmarks/run.py --smoke
```

La prueba corta valida el evaluador; no sirve para sacar conclusiones de
rendimiento. Consulta la [metodología y los comandos de evaluación](benchmarks/README.md)
para configurar escenarios, carga y repeticiones, y leer sus resultados.
