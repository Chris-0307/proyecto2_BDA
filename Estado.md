# Estado del Proyecto 2 — BDA

**Actualización:** 24/11/2026  
**Repositorio:** [proyecto2_BDA](https://github.com/Chris-0307/proyecto2_BDA)

Mi parte (Sesión, Valkey e Historial Parquet) ya está implementada, probada y documentada en el informe. Dejo el estado de las pruebas y los pendientes que identifiqué en la última versión del LaTeX para que podamos cerrar la entrega.

## Christian — Valkey e Historial Parquet

- [x] Configuración de Valkey con AOF `everysec` y volumen persistente.
- [x] Locks exclusivos de 120 s, renovación, liberación, sesiones y borradores.
- [x] Recuperación de borradores, vencimiento de locks y reasignación sin doble propietario.
- [x] Recepción idempotente de eventos, Valkey Streams y confirmación mediante `XACK`.
- [x] Escritura Parquet por mes, publicación atómica, reprocesamiento mediante `XAUTOCLAIM` y deduplicación por `event_id`.
- [x] P3, P4 y P8 de Valkey ejecutadas sobre el Compose final.
- [x] Estrategia de compactación manual documentada (no se implementó compactación automática ni se midió su rendimiento).
- [x] Resultados, limitaciones, evidencias y conclusiones incorporados al informe (secciones 6, 7, 10, 12.2, 13, 14 y 16).

### Evidencias de mi parte

| Prueba | Comando | Resultado | Archivo en `proyecto2/evidence/` |
|---|---|---|---|
| P3 — Sesión | `make p3` | 29/29; borrador de 2,036 s; recuperación tras reinicio en 1,179 s | `p3_gateway_sesion.txt` |
| P4 — Historial | `make p4` | 27/27; 3 filas físicas → 2 eventos únicos; frescura de envíos de 2,177 y 2,366 s | `p4_gateway_parquet.txt` |
| P8 — Valkey | `make p8-valkey` | 31/31; sin pérdida de versiones de borrador observada en los tres escenarios | `p8_valkey_fallas.txt` |

**Total: 87/87 comprobaciones correctas en las tres ejecuciones exitosas.** Los comandos `make` son los equivalentes reproducibles en Linux/WSL; las pruebas se ejecutaron desde Windows con Python, 12 CPU lógicos y 7,73 GiB disponibles para Docker.

P4 midió la visibilidad directa de los eventos en Spark. La frescura específica hasta el resultado de Q1 queda como pendiente de integración con Analítica (ver abajo); no debe confundirse con la prueba P4 ya realizada.

## Jimmy — Registro, MongoDB y Gateway

Según el informe, P1, P2 y P8 MongoDB ya tienen resultados documentados. Pendientes que quedan por revisar:

- [ ] **Sección 5.1 — Planes `explain` de MongoDB.** Capturar los planes de candidatas de `/tareas/siguiente`, tareas por anotador, tareas por lote y exportación. Documentar índice utilizado, documentos examinados y devueltos. Guardar la evidencia y sustituir el recuadro de pendiente.
- [ ] **Sección 13 — Ruta de Analítica.** Eliminar o actualizar el pendiente de `/analitica/{q1..q5}`: el informe ya reporta solicitudes a esa ruta en P6. Confirmar su funcionamiento en el Compose final.
- [ ] **Sección 1 — Diagrama de arquitectura.** Quitar el estilo de pendiente del nodo Spark y dibujar la conexión Gateway → Analítica (puerto interno 8004), manteniendo la lectura exclusiva de Parquet.
- [ ] **Sección 3 — Aislamiento.** Revisar si la evidencia P1 verifica también que el puerto 8004 no esté publicado; documentarlo si se comprobó o completar la prueba.
- [ ] **Sección 10 — P1.** Aclarar la frase «Falta el tráfico del simulador», considerando que P6 reporta tráfico HTTP sobre el Gateway. Coordinar con Emmanuel la evidencia de que el simulador utiliza únicamente el Gateway.

## Emmanuel — Dataset, Analítica Spark y carga

### P5 — Analítica (prioridad alta)

- [ ] Revisar que la implementación efectiva de Q1–Q5 en la rama final coincida con las cinco preguntas del enunciado y con las definiciones de la sección 9. No basta con que devuelvan HTTP 200.
- [ ] Validar Q1–Q5 con un micro-dataset controlado y resultados esperados conocidos, incluyendo duplicados por `event_id`, rechazos, duraciones y SLA. Guardar la evidencia.
- [ ] Capturar los `explain` completos de Q1 y Q3, incluyendo agregaciones finales y ordenamiento donde corresponda; los actuales cubren la lectura y deduplicación.
- [ ] Unificar los conteos de filas de las dos tablas de la sección 9: Q2 aparece como 50 y 120; Q4 como 50 y 720; Q5 como 50 y 180. Confirmar contra `p5_analitica.txt` y la versión del código utilizada. En particular, verificar por qué Q5 tendría 180 filas si agrupa únicamente por cliente y el dataset indica 60 clientes.
- [ ] **Frescura hasta Q1 (integración Historial–Analítica).** Obtener el conteo base de Q1 para el día y modalidad de una tarea de prueba, aprobarla vía Gateway y medir desde HTTP 200 de revisión hasta que Q1 refleje el incremento esperado. Usar un conjunto que incluya el evento reciente (revisar el filtro histórico/ventana de 90 días), evitar otras aprobaciones concurrentes y guardar las mediciones. La prueba P4 existente ya comprueba visibilidad directa en Spark; falta este criterio adicional de la rúbrica.
- [ ] Actualizar las conclusiones de Spark (sección 16): los rangos de 24,23–27,59 s y 15,34–24,51 s no coinciden con la tabla actual de segundas ejecuciones de la sección 9. Verificar los valores originales antes de reemplazarlos (la tabla actual indica 38,40–57,94 s y 14,84–32,08 s, respectivamente).

### Dataset y P6 — Carga

- [ ] **Sección 8.** Registrar los conteos reales de las entidades de MongoDB y del historial generado, además de comprobar los comandos de carga de `make datos` contra las opciones del generador y el Compose actual.
- [ ] **Secciones 10 y 11.** Revisar que P6 documente los 30 s de calentamiento por nivel según el enunciado, los envíos confirmados por minuto, los errores y las tasas de error, además de las latencias y `docker stats` ya incluidos. Completar los datos que falten a partir de la evidencia.
- [ ] Si los recursos y el tiempo lo permiten, repetir P6 con suficientes tareas disponibles en cada repetición para comparar 10, 40 y 100 usuarios bajo trabajo efectivo. Las corridas agotadas y los timeouts actuales son resultados válidos, pero no demuestran la capacidad máxima del sistema. Si no se repite, mantener explícita la limitación.
- [ ] Verificar las fechas de inicio y fin de P5/P6 en la sección 14: P5 tiene una ventana distinta en la sección 9 y P6 usa como inicio una hora descrita como finalización en la sección 11.

El conjunto ampliado de aproximadamente 10 millones de eventos es opcional; no es necesario generarlo si los recursos no lo permiten y se mantiene documentada la limitación.

## Pendientes generales — cierre de la entrega

- [ ] Integrar las ramas de trabajo en la rama de entrega y comprobar que todos los scripts, cambios de Makefile y archivos de evidencia estén presentes (incluidos `p3`, `p4` y `p8-valkey`).
- [ ] Actualizar el README general con los comandos de arranque, detención, carga, consultas y pruebas reproducibles; revisar que no queden instrucciones obsoletas.
- [ ] Ejecutar una revisión final del Compose y las suites necesarias (`make i2-pytest` incluida) sobre la rama de entrega. No sobrescribir las evidencias anteriores sin conservar su trazabilidad.
- [ ] Corregir el entorno de medición de la portada (línea 92): distinguir el entorno WSL2 de 16 CPU lógicos del entorno Windows de 12 CPU lógicos y 7,73 GiB de Docker utilizado en P3/P4/P8 Valkey.
- [ ] Actualizar los comentarios iniciales del LaTeX, que todavía enumeran como pendientes secciones ya completadas, y revisar referencias a apéndices (se menciona el apéndice A, pero no aparece en el documento actual).
- [ ] Revisar que las secciones 10, 13, 14 y 16 coincidan con las evidencias finales, sin afirmar mediciones no realizadas; compilar el LaTeX y revisar el PDF antes de entregar.

## Resumen

Mi plan original de Sesión e Historial está cerrado a nivel de implementación, pruebas y documentación. Los pendientes principales son la validación de Analítica, la frescura hasta Q1, los planes de MongoDB y la revisión final del informe y del repositorio. No hace falta implementar nuevas funcionalidades de Sesión ni un compactador automático para cerrar mi plan.

**Nota:** este estado se basa en la última versión del informe compartida, las evidencias P3/P4/P8 y el enunciado. Los pendientes de los demás son los identificados en esa revisión; puede que ya tengan cambios locales sin incorporar al informe. No se ha verificado en esta revisión el merge final de las ramas ni la compilación del PDF.
