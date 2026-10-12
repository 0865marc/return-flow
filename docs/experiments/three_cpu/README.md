# Receta experimental: comparación con 3 CPU de aplicación

Esta receta conserva el ensayo usado en el [caso de estudio](../../../README.md)
para comparar la recepción directa y mediante RabbitMQ. Procede de los scripts
archivados con la ejecución `three-cpu-20261012T115957Z`. Permite repetir el
procedimiento con el código actual; **no garantiza reproducir sus cifras**.

Es un experimento acotado, separado del [runner general](../../../benchmarks/README.md).
Usa k6 porque el generador Python había mostrado límites propios a tasas altas.
El reparto de CPU es explícito: `--app-cpus 3` en el runner general dividiría las
CPU de eventos por igual, y no reproduce por sí solo este ensayo.

## Configuración fija

| Parámetro | Directa (`postgres_pool`) | Eventos (`event_driven`) |
| --- | --- | --- |
| API | 3 CPU, 3 procesos Uvicorn | 2 CPU, 2 procesos Uvicorn |
| Worker | No se inicia | 1 CPU, 8 comandos concurrentes |
| PostgreSQL | 1 CPU | 1 CPU |
| RabbitMQ | No se inicia | 1 CPU |
| Generador | k6 1.6.1, 4 CPU, 512 VUs | Igual |
| Pool PostgreSQL | 10 conexiones por proceso | 10 conexiones por proceso |
| Publicaciones pendientes | No aplica | 64 por proceso de API |
| Carga | 2.000 solicitudes de devolución/s durante 15 s | Igual |
| Repeticiones | Una, ejecutada primero | Una, ejecutada después |
| Espera pasiva | 15 s tras terminar las respuestas HTTP | Igual |

Cada petición tiene una entrega previamente preparada y una clave de idempotencia
distinta. No se encadena el flujo de creación, entrega y devolución durante la
carga. Las 3 CPU son presupuesto de aplicación: base de datos, broker y cliente
tienen las cuotas adicionales de la tabla.

## Ejecución

Requiere Python 3, Docker y Docker Compose, con un daemon local que pueda montar
el repositorio. No necesita k6 ni las dependencias de la API instalados en el host.
Desde la raíz del repositorio:

```bash
python3 docs/experiments/three_cpu/run_pair.py --help
python3 docs/experiments/three_cpu/run_pair.py
```

El primer comando solo muestra la ayuda. El segundo construye la imagen de la
API actual una vez, crea una carpeta única en `benchmarks/results/three-cpu-…`
y ejecuta las dos variantes secuencialmente. Usa proyectos Docker `rfbench-…`
con redes y volúmenes propios; al terminar cada variante elimina únicamente los
recursos de su proyecto. No utiliza ni modifica los datos de desarrollo.

La preparación, construcción de imágenes, espera de respuestas HTTP y parada de
servicios se añaden a los 15 s de envíos y los 15 s de espera pasiva. Para reutilizar
una imagen `return-flow-benchmark-v2:local` ya construida:

```bash
python3 docs/experiments/three_cpu/run_pair.py --no-build
```

La ejecución histórica también leyó contadores de recursos antes y después del
proceso k6. Esa lectura es opcional en la receta publicada:

```bash
python3 docs/experiments/three_cpu/run_pair.py --resource-counters
```

Esta opción requiere **Linux con Docker local, cgroups v2 y permiso para leer
`/proc/<pid>/cgroup` y `/sys/fs/cgroup` del host**. No funciona tal cual con Docker
Desktop ni un daemon remoto. No hace muestreo periódico: toma dos lecturas, antes
del arranque de k6 y después de su finalización. Su intervalo incluye la
inicialización del cliente y el drenaje HTTP; excluye los 15 s de drenaje pasivo.
Sin la opción, el campo `cpu` queda vacío y `counters.json` indica `enabled: false`.

## Método y artefactos

[`run_pair.py`](run_pair.py) reutiliza la preparación, Compose y snapshot del
benchmark general. Antes de la carga comprueba las cuotas efectivas y guarda
las identidades de las imágenes de aplicación e infraestructura. Mantiene los
healthchecks y access logs desactivados. Durante la carga,
[`load.js`](load.js) envía solo POST; no consulta estados ni reintenta peticiones.
k6 realiza comprobaciones locales de respuesta y acumula métricas en memoria;
escribe su resumen al finalizar el proceso HTTP. Este coste local del generador
sigue existiendo.

Después del drenaje pasivo, el runner detiene API y worker, verifica que están
parados y obtiene el snapshot PostgreSQL. El informe conserva aceptación HTTP,
integridad de las entidades y finalización persistida por separado. Los comandos
sin resultado al corte siguen sin resolver; no se clasifican como perdidos.

La salida incluye `comparison.json` y, por variante, `config.json`, `plan.json`,
`compose.resolved.json`, `actual-limits.json`, `k6-summary.json`, `snapshot.json`,
`report.json` y `counters.json`. También archiva los tres scripts. Las carpetas de
resultados quedan fuera de Git; una selección de la ejecución original está en
la [evidencia publicada](../../evidence/performance-2026-10-12.json).

El runner conserva el `k6_exit_code` y los contadores originales incluso cuando
k6 incumple un umbral. Que el script Python llegue al final significa que pudo
generar los informes, no que ambas variantes mantuvieran la carga. Para evaluar
el caso hay que revisar peticiones intentadas y aceptadas, descartes, fallos y
comandos sin completar.

## Análisis posterior del tiempo de finalización

Usa la ruta mostrada en `RESULTS_ROOT` al terminar la ejecución:

```bash
python3 docs/experiments/three_cpu/completion.py benchmarks/results/three-cpu-TU_EJECUCION
```

[`completion.py`](completion.py) solo lee los artefactos y escribe
`completion-timing.json`; no accede a Docker, HTTP, PostgreSQL ni RabbitMQ.
Reconstruye cuántos comandos tenían un timestamp terminal al terminar los envíos
y si todos los intentados llegaron a terminar antes del corte. Si faltan
resultados, el último timestamp observado no es el tiempo de finalización total.

Los timestamps `completed_at` se toman antes del commit. Compararlos con el
inicio UTC del escenario aproxima el corte, sin calibrar el desfase entre el
reloj del cliente y PostgreSQL. k6 no conserva el inicio individual de cada
petición. Solo cuando no hay descartes se estima un inicio mediante el índice
del plan y la tasa; esa aproximación incluye el retraso del generador y no es una
latencia exacta de extremo a extremo.

Se conserva también un detalle del ensayo original: k6 puede iniciar un tick en
el límite final, cuando ya se han recorrido las 30.000 entradas del plan. El
guard de `load.js` vuelve antes de enviar otro POST y registra `plan_errors` y
una iteración fallida. El resumen original puede por ello incumplir un umbral
aunque los 30.000 POST tengan respuestas válidas. El analizador reconoce ese
caso solo si encajan todos sus contadores y lo identifica como iteración sin
petición HTTP; **no modifica ni normaliza los datos originales**.

Una repetición y el orden fijo no estiman variabilidad ni eliminan el efecto del
host compartido. Tampoco el número de operaciones aceptadas demuestra capacidad
de procesamiento sostenida: una cola puede seguir creciendo durante el ensayo.
