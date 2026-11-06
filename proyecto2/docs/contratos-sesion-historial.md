
# Contratos de integración - Sesión e Historial

## 1. Objetivo

Este documento establece las responsabilidades y los contratos
iniciales de los servicios de Sesión e Historial de la plataforma
distribuida de anotación de datos.

Ambos servicios deben integrarse con el gateway y los demás
componentes sin acceder directamente a las bases de datos
que pertenecen a otros servicios.

## 2. Servicio de Sesión

El servicio de Sesión utiliza Valkey para gestionar la
información temporal de las tareas que están siendo anotadas.

Sus responsabilidades son:

- Administrar candados exclusivos sobre las tareas.
- Mantener las sesiones activas de los anotadores.
- Almacenar los borradores de anotación.
- Renovar los candados mediante latidos.
- Recuperar borradores después de una interrupción.
- Liberar candados cuando corresponda.

El servicio de Sesión no puede modificar directamente
el estado definitivo de las tareas en MongoDB.

### 2.1. Estructuras de Valkey

Las estructuras requeridas son:

**lock:tarea:{id}**

Candado exclusivo de una tarea.

- Adquisición mediante SET NX PX.
- Duración inicial de 120 segundos.
- Renovación mediante latidos.
- Validación del token de propiedad.

**sesion:{anotador}**

Hash con información de la sesión:

- Identificador de la tarea.
- Identificador del proyecto.
- Etapa de trabajo.
- Token de asignación.

**borrador:{tarea}**

Hash que almacena la última versión del borrador
de una tarea.

El borrador no constituye una anotación definitiva y
no genera eventos para el datalake.

**editando:{proyecto}**

Conjunto ordenado que permite consultar las sesiones
activas de un proyecto.

### 2.2. Persistencia

Valkey debe utilizar persistencia AOF con:

- appendonly yes
- appendfsync everysec

El borrador debe sobrevivir al reinicio del contenedor
mientras no haya sido enviado definitivamente.

### 2.3. Consistencia

MongoDB mantiene el estado definitivo de las tareas.

La asignación exclusiva debe confirmarse mediante una
operación atómica en MongoDB.

Si falla la asignación en MongoDB, el candado adquirido
en Valkey debe liberarse.

Las operaciones sobre candados deben comprobar el token
para impedir que otro anotador libere o renueve
una sesión ajena.

## 3. Servicio de Historial

El servicio de Historial almacena eventos de negocio
en un datalake de archivos Parquet.

Sus responsabilidades son:

- Consumir eventos generados por las operaciones.
- Agrupar eventos en micro-lotes.
- Escribir archivos Parquet.
- Particionar el historial por mes.
- Publicar únicamente archivos completos.
- Permitir el reprocesamiento de eventos.
- Conservar identificadores para deduplicación.

### 3.1. Organización del datalake

Los eventos se almacenarán en:

proyecto2/datalake/eventos/mes=AAAA-MM/

Cada archivo Parquet contendrá eventos históricos
de las operaciones realizadas.

### 3.2. Publicación de archivos

Los micro-lotes deben escribirse primero en una
ubicación temporal.

Cuando termina la escritura, el archivo se publica
con su nombre definitivo.

Esto evita que Apache Spark lea archivos incompletos.

### 3.3. Identificadores de eventos

Cada evento debe contener un identificador event_id.

Este identificador permite que Spark elimine
duplicados cuando se reprocesan eventos.

### 3.4. Transiciones registradas

Las transiciones que deben generar eventos son:

- disponible -> abierta
- abierta -> anotada
- anotada -> en_revision
- en_revision -> aprobada
- en_revision -> disponible (rechazo)

En caso de rechazo:

- MongoDB almacena el estado disponible.
- Parquet registra el evento con estado rechazada.

Las actualizaciones de borradores no generan eventos.

## 4. Integración con los demás servicios

### Gateway

Es el único punto de entrada para el simulador.

Debe comunicarse con los servicios internos
respetando sus responsabilidades.

### Registro

Mantiene los estados definitivos de las tareas
en MongoDB.

La confirmación del registro es necesaria antes
de reconocer un envío como exitoso.

### Analítica

Utiliza Apache Spark para consultar exclusivamente
los archivos Parquet del datalake.

No consulta MongoDB para responder Q1-Q5.

## 5. Contratos pendientes de definir

Antes de implementar la integración se deben acordar:

- Endpoints REST internos.
- Formato JSON de las solicitudes y respuestas.
- Esquema de los eventos históricos.
- Mecanismo de mensajería durable.
- Confirmación y reprocesamiento de eventos.
- Manejo de errores entre Gateway, Registro y Sesión.
- Volúmenes compartidos del datalake.
- Nombres de los servicios en Docker Compose.

Estos contratos se definirán junto con los
integrantes responsables de los demás componentes.

## 6. Pruebas asignadas

### P3 - Sesión

- Recuperación de borradores.
- Persistencia después de reiniciar Valkey.
- Vencimiento de candados.
- Prevención de doble propiedad.

### P4 - Historial

- Escritura de archivos Parquet válidos.
- Reprocesamiento de micro-lotes.
- Deduplicación por event_id.
- Publicación de archivos completos.
- Medición del tiempo hasta que Spark lee el evento.

Las evidencias se almacenarán en proyecto2/evidence/.
