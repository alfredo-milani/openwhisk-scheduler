* Risultati tests:
    1. Nel caso di siano tutti containers di tipo A in pausa e arriva una action di tipo B, un container di tipo A viene distrutto e viene creato un container di tipo B, senza segnalare che il sistema è overloaded.
    2. In caso il container di una action non può essere creato, il controller segnala correttamente che il sistema è in overload.
    3. Ci sono 8 containers di tipo A (8 = #containers massimi che possono essere creati su un invoker) su invoker: 7 stanno processando
        2 activations ciascuno (con 2 = livello massimo di concorrenza), l'ottavo sta processando 1 activation e arrivano 3 richieste
        nel sistema: 1 richiesta di action di tipo B e 2 richieste di action di tipo A ==> nel caso in cui nel sistema ci sia 1 solo invoker,
        allora la richiesta di tipo B viene accoda sull'invoker, che dal punto di vista del controller risulta saturo perché non può
        ospitare la action di tipo B, la prima richiesta di tipo A viene servita dall'ottavo container e la terza richiesta di tipo A
        viene accodata dal momento che non ci sono più containers o livello di concorrenza sufficienti.
    NOTA: è strano il fatto che, anche se tutte a 3 le action abbiano sleep time pari a 1, la terza action (di tipo A) venga completata assieme alle altre actions in esecuzione sull'invoker, anche se queste hanno uno sleep time molto maggiore (55 s vs 1 s).
    Test log in ./2
    4. Solo runtime nodejs (14, 12, 10) supporta concorrenza (see@ https://github.com/apache/openwhisk/blob/master/docs/concurrency.md). In realtà, dai tests effettuati, è risultato che anche il runtime python:3 supporta concorrenza (openwhisk/python3action:latest
    5. Ci sono 8 containers di tipo A (8 = #containers massimi che possono essere creati su un invoker) su invoker: 7 containers processano
        sleep(55), l'ottavo processa sleep(5); arrivano 2 actions: una di tipo B e una di tipo A.
        Viene processata prima quella di tipo B e poi quella di tipo A.
        Nota: far accodare activations su invoker per fare tests è molto difficile... Dipende molto anche dalla variabilità di rete.
        Test log in ./5 (Nota: è stata utilizzata la versione base di OpenWhisk, senza Scheduler)
); tests effettuati con action sleeps/sleep_one, livello di concorrenza 5 - invocando in modo sequenziale 11 action vengono creati 3 containers.
    n. Dopo 50 ms il container viene messo in pausa; dopo 10 min il container viene distrutto



* Links utili:
    - https://stackoverflow.com/questions/57281576/openwhisk-invoker-agent-suspend-time
    - https://github.com/apache/openwhisk/issues/3516
