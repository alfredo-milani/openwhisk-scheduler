Per evitare che, una volta utilizzato il comando helm, il container termini con exit status 126, è necessario creare il container affinché fornisca il servizio per cui è stato pensato. In modo alternativo, è possibile specificare, nei template helm relativi alla definizione dei pod, un comando da eseguire una volta che il container è stato avviato.

- Soluzione 1: https://stackoverflow.com/questions/31870222/how-can-i-keep-a-container-running-on-kubernetes
    - specificando comandi ed argomenti, e.g.:
    apiVersion: v1
    kind: Pod
    metadata:
      name: ubuntu
    spec:
      containers:
      - name: ubuntu
        image: ubuntu:latest
        # Just spin & wait forever
        command: [ "/bin/bash", "-c", "--" ]
        args: [ "while true; do sleep 1d; done;" ]

    - Nota: il comando 'sleep' può accettare argomenti in formato diverso in base al tipo di immagine Docker utilizzata.
    - Comando alternativo:
        command: [ "/bin/bash", "-c", "--" ]
        args: [ "trap: TERM INT; sleep 1d & wait" ]

- Soluzione 2: https://stackoverflow.com/questions/44140593/how-to-run-command-after-initialization
    - utilizzando la direttiva 'lifecycle', e.g.:
    apiVersion: extensions/v1beta1
    kind: Deployment
    metadata:
      name: auth
    spec:
      replicas: 1
      template:
        metadata:
            labels:
              app: auth
        spec:
          containers:
            - name: auth
              image: {{my-service-image}}
              env:
              - name: NODE_ENV
                value: "docker-dev"
              resources:
                requests:
                  cpu: 100m
                  memory: 100Mi
              ports:
                - containerPort: 3000
              lifecycle:
                postStart:
                  exec:
                    command: ["/bin/sh", "-c", {{cmd}}]