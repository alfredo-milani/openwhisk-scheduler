# Table of contents

1. [Introduction](#introduction)
2. [Requirements](#requirements)
3. [Installation](#installation)



## 1. Introduction <a name="introduction"></a>

The goal of this project is to introduce service differentiation mechanisms within [Apache OpenWhisk](https://github.com/apache/openwhisk) framework.
General information about this research project is available [here](https://alfredo-milani.github.io/openwhisk-scheduler/).

We extend OpenWhisk by introducing a Scheduler component, which becomes responsible for the load balancing originally performed by the Controller and supports additional scheduling policies. In the new architecture, incoming requests are not directly scheduled by the Controller to the Invokers and are instead sent to the Scheduler. The newly introduced component communicates with both, the Controller and the Invokers, through Kafka. Within the Scheduler, a Consumer component continuously waits for events from Kafka. The two main types of events received by the Consumer are arrivals and completions. Arrival events are published by the Controller upon reception of new requests and cause the associated activations to enter the Scheduler buffer. Conversely, completion events are published by Invokers when activation processing is complete. This information is used by the Controller in the base version of OW and also exploited by our Scheduler to keep track of the Invoker workload.
A Policy component manages the scheduler buffer, supporting the enforcement of different admission and scheduling policies. When the policy selects an activation from the buffer to be executed on a certain Invoker, a Producer takes care of communicating the required information to the Invoker through Kafka.

## 2. Requirements <a name="requirements"></a>

[JDK 13](https://www.oracle.com/java/technologies/javase/jdk13-archive-downloads.html) or higher.

[SDK Apache Kafka 2.7+](https://kafka.apache.org/27/javadoc/overview-summary.html)

## 3. Installation <a name="installation"></a>

#### Compiling code

```shell
# clone repository
git clone https://github.com/alfredo-milani/openwhisk-scheduler
# move to root directory
cd openwhisk-scheduler
# compile sources
mvn clean && mvn package
```

#### Deploy

The deployment was done using Google's managed service, GKE, following the instructions provided by the OpenWhisk developers, [openwhisk-deploy-kube](https://github.com/apache/openwhisk-deploy-kube).

The new architecture introduces a new component (the Scheduler), so changes to the Helm charts used to deploy the framework were necessary. The directory `res/openwhisk-deploy-kube` hosts some of the configurations used for deployment.
