# Return-Flow

Simulación práctica de un sistema de entregas y devoluciones con Python y FastAPI,
PostgreSQL y RabbitMQ. El proyecto utiliza arquitectura hexagonal y eventos
asíncronos, con un entorno de desarrollo local basado en Docker Compose.

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

El servicio `api` recibe `DATABASE_URL` y `RABBITMQ_URL` con los nombres internos de
los servicios. La conexión a PostgreSQL y RabbitMQ se implementará con la lógica de
la aplicación; `/health` solo comprueba que la API responde.

Para detener los servicios: `docker compose down`. Los datos se conservan en volúmenes; `docker compose down -v` también los elimina.

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
- **`main.py`:** arranca la API y conecta las piezas.

Por ejemplo, una petición llega a FastAPI, que llama a un caso de uso. Este aplica
las reglas del dominio y, si necesita guardar datos, utiliza un puerto. El
adaptador de PostgreSQL implementa ese contrato y realiza la operación.

La regla de organización es que el dominio no necesita conocer HTTP, SQL ni
RabbitMQ, y los casos de uso conocen los contratos, no sus implementaciones.

Esta es la estructura prevista. Actualmente están implementados el dominio y el
arranque de FastAPI; `application/` y `adapters/` se añadirán conforme avancemos.

```text
api/
├── main.py
├── domain/
├── application/
│   ├── use_cases/
│   └── ports/
└── adapters/
    ├── http/
    ├── persistence/
    └── messaging/
```

## Testing

Las pruebas usan `pytest` y siguen la misma organización que el código de la API:

```text
api/tests/
├── domain/
├── application/
└── adapters/
```

Para ejecutarlas en Docker, desde la raíz del proyecto:

```bash
docker compose up -d --build api
docker compose exec api python -m pytest
```
