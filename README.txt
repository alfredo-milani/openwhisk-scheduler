* Con whisk/controller:pqv1, tutte le activation sono indirizzate verso il topic 'to_dispatch', quindi se si prova ad invocare una action, il sistema ritorna un errore dicendo che è sovraccarico. Il deploy delle actions funziona normalemnte.

Errore ricevuto: "error: Unable to invoke action 'hello_py': The server is currently unavailable (because it is overloaded or down for maintenance). (code eEvvuAGj9m678z4ZVkcdu7SsGHKz9rIj)"









USAGE

Per eseguire l'applicazione con il comando java:
- $ java -cp target/classes:/Users/alfredo/.m2/repository/org/apache/logging/log4j/log4j-core/2.12.1/log4j-core-2.12.1.jar:/Users/alfredo/.m2/repository/org/apache/logging/log4j/log4j-api/2.12.1/log4j-api-2.12.1.jar it.uniroma2.faas.openwhisk.scheduler.Application
Nota: è necessario specificare tutte le dipendenze necessarie per l'esecuzione

NOTE:
- zero bugs commitment, see@ https://github.com/classgraph/classgraph/blob/latest/Zero-Bugs-Commitment.md
- versioning, see@ https://semver.org/lang/it/