Redactar un breve informe en el archivo `INFORME.md` explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes, grándes volúmens de datos y la cantidad de controles.


# Informe - TP Coordinación

## 1. Introducción

El presente trabajo implementa un sistema distribuido de control de stock para una verdulería. El sistema recibe pares `(fruta, cantidad)` desde múltiples clientes concurrentes y devuelve a cada uno un top K de frutas según la mayor cantidad total acumulada.

La arquitectura sigue un modelo de pipeline con cuatro partes involucradas: Gateway, Sum, Aggregation y Join. El objetivo principal es que el sistema escale en tres dimensiones:

- **Clientes concurrentes**: múltiples clientes pueden enviar datos simultáneamente sin interferir entre sí.
- **Volumen de datos**: la carga de procesamiento se distribuye entre las instancias disponibles.
- **Cantidad de controles**: el sistema se adapta dinámicamente a la cantidad de instancias de Sum y Aggregation configuradas en el docker-compose.

Las pruebas incrementales otorgadas por la cátedra prueban que el sistema cumple con cada uno de los objetivos del trabajo.

## 2. Arquitectura del Modelo

En cada conexión del cliente al sistema, se maneja el siguiente flujo de datos:

```
Cliente ──TCP──► Gateway ──input_queue──► Sum ──exchange──► Aggregation ──join_queue──► Join ──results_queue──► Gateway ──TCP──► Cliente
```

### Client

Lee un archivo de entrada y envía por TCP/IP pares (fruta, cantidad) al sistema. Cuando finaliza el envío de datos, aguarda un top de pares (fruta, cantidad) y vuelca el resultado en un archivo de salida csv. El criterio y tamaño del top dependen de la configuración del sistema. Por defecto se trata de un top 3 de frutas de acuerdo a la cantidad total almacenada.

### Gateway

Es el punto de entrada y salida del sistema. Recibe las conexiones TCP de cada cliente, asigna un `clientID` a cada uno, publica en la `input_queue` y entrega el resultado final.

### Sum

Recibe pares (fruta, cantidad) y aplica la función Suma de la clase `FruitItem`. Cuando se detecta el final de la ingesta de datos envía los pares (fruta, cantidad) totales a los Aggregators.

### Aggregator

Consolida los datos de las distintas instancias de Sum. Cuando se detecta el final de la ingesta, se calcula un top parcial y se envía esa información al Joiner.

### Joiner

Recibe tops parciales de las instancias del Aggregator. Cuando se detecta el final de la ingesta, se envía el top final hacia el gateway para ser entregado al cliente.

Cada mensaje incluye el `clientID` como primer elemento, seguido de los pares `(fruta, cantidad)`. Un mensaje sin pares indica fin de ingesta (EOF) para ese cliente.

## 3. Coordinación de Sum

Cada Sum recibe mensajes por dos goroutines concurrentes distintas: `input_queue` (datos) y su cola de control. Los mensajes de **datos** (pares fruta-cantidad) viajan por `input_queue`. Los mensajes de **control** (propagación del EOF) viajan por un canal separado implementado como un anillo de colas de control.

El diseño de dos canales independientes permite que el flujo de datos y el de control se coordinen sin interferirse, al mismo tiempo que permite coordinación entre las múltiples instancias de Sum.

### Mecanismo de Coordinación: Anillo de control

Cada instancia de Sum conoce su `ID` y la cantidad total de instancias (`SUM_AMOUNT`), datos provenientes de las variables de entorno. Con esos datos calcula su sucesor en el anillo como `(ID + 1) % SUM_AMOUNT`. Cada Sum tiene una **cola de control privada** (`{SUM_PREFIX}_{ID}_control`) de la que consume, y produce en la cola del sucesor.

El protocolo funciona así:

1. El Gateway publica el EOF original en `input_queue`.
2. El Sum que lo recibe por round-robin lo detecta, arranca el flush de sus datos y publica un **token** (un EOF sin datos) en la cola de control de su sucesor.
3. Cada Sum que recibe el token hace su propio flush y lo propaga.
4. Cuando el token vuelve al Sum que lo originó, este detecta que ya completó su ciclo (`sendingEOF[clientID]`) y no lo reenvía. El anillo se corta.

Esta estrategia garantiza que **todas** las instancias de Sum reciban la notificación de fin de ingesta.

Cuando un token llega, el Sum puede estar procesando el último mensaje de datos del cliente. Para evitar que el flush se ejecute antes de que ese mensaje se procese, se utiliza un `sync.Mutex` que serializa el acceso al estado del cliente.

Con `prefetch=1` nos aseguramos de que cada Sum tenga a lo sumo un mensaje sin confirmar. Esto garantiza que, cuando el token llegue, el Sum esté procesando a lo sumo un mensaje del cliente. Por otro lado, para manejar la concurrencia dentro de un mismo Sum se usa el mutex que asegura que ese mensaje se procese antes del flush.

Cabe destacar que si una instancia de Sum se cae, el token no se propaga y el anillo se corta. El Aggregator nunca recibe los `SUM_AMOUNT` EOFs esperados, y el cliente no obtiene respuesta. Si bien el objetivo del trabajo es coordinar nodos en un sistema distribuido, el manejo de fallas y particiones de red queda fuera del alcance del mismo. Una versión productiva requeriría detección de fallos y reenvío del token.

Al vaciar los datos de un cliente, cada Sum calcula la partición del Aggregation correspondiente para cada fruta mediante:

```
hash(fruta) % AGGREGATION_AMOUNT
```

Como todas las instancias de Sum aplican la misma función de hash, todas las apariciones de una misma fruta terminan en el mismo Aggregator. Esto permite que el Aggregator sume correctamente los totales, y elimina el broadcast a todas las instancias que hacía el esqueleto original, evitando así la redundancia de cómputo y mensajes.

El EOF, en cambio, se envía a **todas** las particiones para que cada Aggregator sepa cuándo terminó el cliente.

## 4. Coordinación de Aggregation

Cada instancia de Aggregation escucha su propia routing key del exchange (`{AGGREGATION_PREFIX}_{ID}`). Por el hash routing, recibe únicamente las frutas que le corresponden por partición.

Cuando recibe `SUM_AMOUNT` EOFs para un cliente, sabe que todas las instancias de Sum terminaron. Calcula entonces su **top parcial** (las `TOP_SIZE` frutas más grandes de su partición) y lo envía al Join, seguido de un EOF propio.

## 5. Coordinación de Join

El Join recibe un top parcial por cada instancia de Aggregation. Como el particionado por hash garantiza que cada fruta vive en un único Aggregator, los tops parciales no se solapan: no hay frutas repetidas entre ellos.

Al recibir cada top parcial, el Join lo concatena con el top acumulado para ese cliente, ordena el resultado y trunca al `TOP_SIZE`. Cada top parcial está garantizado como correcto para su partición, por lo que este merge progresivo produce el top global correcto.

El Join también cuenta EOFs por cliente. Cuando recibe `AGGREGATION_AMOUNT` EOFs, sabe que todos los Aggregators terminaron, envía el top final al Gateway y libera el estado del cliente.

## 6. Escalabilidad

Cada conexión en el Gateway genera un `MessageHandler` con un `clientID` único. Ese identificador viaja en todos los mensajes internos y permite que Sum, Aggregation y Join mantengan estado separado por cliente en mapas indexados por `clientID`. Esto permite que el sistema pueda manejar múltiples clientes concurrentes ya que sus flujos se procesan en paralelo y sus estados viven en estructuras distintas.

Los Sums comparten `input_queue`. RabbitMQ balancea automáticamente los mensajes entre ellos con round-robin. Agregar más instancias de Sum incrementa linealmente la capacidad de procesamiento.

Toda la configuración es dinámica, derivada de variables de entorno:

- `SUM_AMOUNT` y `SUM_PREFIX` para las instancias de Sum.
- `AGGREGATION_AMOUNT` y `AGGREGATION_PREFIX` para las instancias de Aggregation.

El sistema se adapta a cualquier combinación sin cambios en el código, como lo demuestran los cinco escenarios provistos.

## 7. Decisiones de diseño

### Extensión del middleware con SendToKey

Para rutear frutas a particiones específicas, se agregó un método `SendToKey` al tipo concreto `ExchangeMiddleware`, sin modificar la interfaz `Middleware`. Esto sigue la indicación de la cátedra de que sumar métodos a la clase que implementa una interfaz es válido mientras no se rompa el contrato.

El método `Send` sigue disponible y publica a todas las keys configuradas; se usa para el broadcast del EOF.

### Alternativas evaluadas para la coordinación

Al momento de diseñar algoritmos para solucionar los problemas de coordinación entre nodos se barajaron distintas propuestas. Más allá de haber optado por el anillo de control, cabe destacar algunas otras propuestas que podrían resultar interesantes en otros escenarios.

Alternativa 1: El Sum que recibe el EOF original publique el token a un exchange, distribuyéndolo a N colas privadas simultáneamente. Así, cada uno de los N nodos recibe de su propia cola el mensaje de control. Se optó por el anillo porque distribuye la carga de publicación entre los nodos y no requiere un punto central ni conocer las colas de todos los pares de antemano.

El mecanismo del anillo asume `prefetch=1` en los consumidores. Bajo esta restricción, cuando el Sum que recibe el EOF lo detecta, todos los datos previos ya fueron entregados por la cola y cada Sum tiene a lo sumo un mensaje en vuelo. El mutex serializa ese mensaje con el flush.

Alternativa 2:
Para soportar `prefetch>1` se evaluó un protocolo de dos fases:

- **Fase de conteo**: el Gateway incluye en el EOF el total de mensajes enviados para el cliente. Un token circula acumulando los conteos de mensajes procesados por cada Sum. Cuando el total acumulado alcanza el enviado, se sabe que todos los mensajes fueron procesados.
- **Fase de flush**: un segundo token indica a los Sums que vuelquen su estado.

Este protocolo es correcto y no requiere `prefetch=1`, pero incrementa la complejidad y la cantidad de vueltas del anillo. Dado que se permitía trabajar con un `prefetch=1`, se optó por implementar una versión más sencilla.

## 8. Conclusión

La implementación resuelve las falencias identificadas en el esqueleto original:

- Implementa la interfaz del middleware con dos tipos concretos (cola y exchange).
- Separa los flujos de datos de los clientes mediante un `clientID` presente en todos los mensajes internos.
- Coordina la finalización de la ingesta entre múltiples instancias de Sum mediante un anillo de control.
- Distribuye el cómputo entre instancias de Aggregation mediante hash routing de frutas.
- Maneja SIGTERM en todos los componentes con cierre ordenado de recursos.
