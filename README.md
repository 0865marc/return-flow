# Return-Flow

Este proyecto pretende ser simplemente una simulacion práctica-teórica de un sistema de entregas y devoluciones. Este sistema va a utilizar el stack tecnológico de Python (FastAPI) + PostgreSQL, utilizando arquitectura hexagonal + eventos asíncronos (con RabbitMQ) para mejorar la fiabilidad y robustez del servicio. Toda esta infrastructura correra encima de docker-compose de forma totalmente local para este ejemplo.

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
