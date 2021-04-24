Nel caso di 4 invokers (con container factory kubernetes) e 2 controller, nei topics di tutti e 4 gli invoker ci sono activations records provenienti da entrambe le istanze del controller. Inoltre, in entrambi i topics dei controllers ci sono records provenienti da tutti e 4 le istanze degli invokers.

Questo perché, ad ogni Load Balancer (componente del Controller) sono assegante metà risorse di ogni invoker (see@ ShardingContainerPoolBalancer.scala).

Inoltre, i records nei topics 'completedN' sono pubblicati dagli invokers (see@ MessagingActiveAck.scala and see@ InvokerReactive.scala).