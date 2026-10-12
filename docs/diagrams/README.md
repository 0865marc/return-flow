# Diagramas del caso de estudio

Los `.mmd` son las fuentes editables en Mermaid. Los `.svg` son sus vistas
generadas, utilizadas en el README y las guías para que también sean legibles
en visores Markdown sin soporte Mermaid. Se regeneran al cambiar la fuente.

| Diagrama | Qué explica |
| --- | --- |
| [Contexto](system-context.mmd) · [SVG](system-context.svg) | Los recorridos directo y encolado sobre la misma base de datos. |
| [Dependencias](hexagonal-dependencies.mmd) · [SVG](hexagonal-dependencies.svg) | Puertos, casos de uso, dominio y fronteras reales de los adaptadores. |
| [Secuencia directa](direct-command-sequence.mmd) · [SVG](direct-command-sequence.svg) | Transacción compartida y respuesta posterior al commit. |
| [Secuencia encolada](queued-command-sequence.mmd) · [SVG](queued-command-sequence.svg) | Publisher confirm, respuesta 202 y ACK posterior al commit. |
| [Reentrega](rabbitmq-command-redelivery.mmd) · [SVG](rabbitmq-command-redelivery.svg) | Caídas antes y después del commit, con reutilización del resultado. |
| [Fases del benchmark](benchmark-phases.mmd) · [SVG](benchmark-phases.svg) | Preparación, ejecución sin polling y medición tras la parada. |
| [Recepción observada](acceptance-results.mmd) · [SVG](acceptance-results.svg) | Respuestas válidas por segundo frente a la carga ofrecida. |

Las secuencias de ejecución muestran el caso correcto y su reintento con el mismo
contenido. Los demás errores están descritos en [arquitectura](../architecture.md).
La gráfica procede del [extracto de evidencia](../evidence/performance-2026-10-12.json):
19.719 y 30.000 respuestas divididas por 15 segundos. No representa operaciones
de negocio completadas por segundo.

## Regenerar

Se validaron y renderizaron los siete diagramas con **Mermaid CLI 11.17.0**
(motor Mermaid 11.17.2), y se revisaron visualmente sus vistas previas. La
[configuración compartida](mermaid-config.json) define colores, tipografía y
disposición; no modifica el significado de las fuentes.

Con Node.js 20 o superior y Chromium disponible para Puppeteer, desde la raíz:

```bash
for diagram in docs/diagrams/*.mmd; do
  npx --yes --package @mermaid-js/mermaid-cli@11.17.0 mmdc \
    -i "$diagram" -o "${diagram%.mmd}.svg" \
    -c docs/diagrams/mermaid-config.json -b white
done
```

La primera ejecución puede descargar las herramientas. Si se utiliza un Chrome
ya instalado, se puede añadir `-p` con la configuración de Puppeteer apropiada
para ese entorno. El renderizado no requiere la API ni los servicios del proyecto.

Los comentarios `portable-canonical-v2` de algunos flowcharts son metadatos del
editor que los generó; no son necesarios para renderizar. Si se edita a mano una
fuente, deben actualizarse con ese editor o retirarse para no conservar metadatos
incompatibles con el dibujo.
