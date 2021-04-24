TESTS:
* [1]: con replica = 1 (default) e charts di default (openwhisk-deploy-kube) e 2 nodi invoker con 2.5 GiB di memoria (userMemory=2048), viene deployato 1 solo invoker
    - Nota: è stato usato Kubernetes come container factory
    - Nota: i topic 'completed1' non viene proprio creato
    - test: for i in {1..20}; do   wsk-act-binv sleep_one -p sleep 40 & done (sleeps/sleep_one; conc=1); vengono creati 16 containers
    - quindi il sistema, anche se ha risorse necessarie per deployare più invokers, ne deploya un numero uguale a 'replicaClount'
* [2]: con replica = 2 e charts di default (openwhisk-deploy-kube) e 2 nodi invoker con 2.5 GiB di memoria (userMemory=2048)
    - Nota: è stato usato Kubernetes come container factory
    - Nota: i topic 'completed1' non viene proprio creato
    - test: for i in {1..20}; do   wsk-act-binv sleep_one -p sleep 40 & done (sleeps/sleep_one; conc=1); vengono creati 16 containers
    - quindi, il sistema, crea 2 invokers
* [3]: con replica = 1 (default) e charts di default (openwhisk-deploy-kube) e 2 nodi invoker con 4.5 GiB di memoria (userMemory=2048), viene deployato 1 solo invoker
    - Nota: è stato usato Kubernetes come container factory
    - Nota: i topic 'completed1' non viene proprio creato
    - quindi il sistema, anche se ha risorse necessarie per deployare più invokers, ne deploya un numero uguale a 'replicaClount'
* [4]: con replica = 2 e charts di default (openwhisk-deploy-kube) e 2 nodi invoker con 4.5 GiB di memoria (userMemory=2048)
    - Nota: è stato usato Kubernetes come container factory
    - Nota: i topic 'completed1' non viene proprio creato
    - test: for i in {1..20}; do   wsk-act-binv sleep_one -p sleep 40 & done (sleeps/sleep_one; conc=1); vengono creati 16 containers
    - quindi, il sistema, crea 2 invokers
* [5]: con replica = 1 (default) e charts di default (openwhisk-deploy-kube) e 2 nodi invoker con 3 GiB di memoria (userMemory=2048)
    - Nota: è stato usato Docker come container factory
    - Nota: i topic 'completed1' non viene proprio creato
    - Nota: con docker come container factory, con il comando 'launcher ow wa none' non vengono visualizzati i containers creati per gestire le user actions
    - test: for i in {1..20}; do   wsk-act-binv sleep_one -p sleep 40 & done (sleeps/sleep_one; conc=1); vengono creati 16 containers (forse)
    - quindi, il sistema, crea 2 invokers
* [6]: con replica = 2 e charts di default (openwhisk-deploy-kube) e 2 nodi invoker con 2.5 GiB di memoria (userMemory=2048)
    - Nota: è stato usato Kubernetes come container factory
    - Nota: il topic 'completed1' viene creato
        - Quindi i topics 'completedN' sono associati ai controllers e non agli invokers
    - test: for i in {1..20}; do   wsk-act-binv sleep_one -p sleep 40 & done (sleeps/sleep_one; conc=1); vengono creati 16 containers (forse)


LINKS UTILI (dal repo github di Apache OpenWhisk):
    - https://github.com/apache/openwhisk-deploy-kube
    - https://github.com/apache/openwhisk-deploy-kube/blob/master/docs/k8s-google.md#configuring-openwhisk
    - https://github.com/apache/openwhisk-deploy-kube/blob/master/docs/k8s-custom-build-cluster-scaleup.md
    - https://github.com/apache/openwhisk-deploy-kube/blob/master/docs/configurationChoices.md
