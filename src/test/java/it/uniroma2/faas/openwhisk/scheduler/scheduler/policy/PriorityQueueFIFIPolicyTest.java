package it.uniroma2.faas.openwhisk.scheduler.scheduler.policy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Activation;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.ISchedulable;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class PriorityQueueFIFIPolicyTest {

    @Test
    public void isPriorityQueueFIFIPolicySortingCorrectly() {
        final Random random = new Random();
        final ObjectMapper objectMapper = new ObjectMapper();
        final Collection<Activation> data = new ArrayDeque<>(20);
        final String activationBufferedScheduler = "{\"action\":{\"name\":\"hello_py\",\"path\":\"guest\",\"version\":\"0.0.1\"},\"activationId\":\"%s\",\"blocking\":true,\"content\":{\"kTest\":\"vTest\",\"$scheduler\":{\"target\":\"%s\",\"priority\":%d,\"limits\":{\"concurrency\":2,\"memory\":256,\"timeout\":60000,\"userMemory\":2147483648}}},\"initArgs\":[],\"lockedArgs\":{},\"revision\":\"1-ff07dcb3291545090f86e9fc1a01b5bf\",\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"4K6K9Uoz09O5ut42VEiFI9zrOAiJX8oq\",1611192819095],\"user\":{\"authkey\":{\"api_key\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"},\"limits\":{},\"namespace\":{\"name\":\"guest\",\"uuid\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"},\"rights\":[\"READ\",\"PUT\",\"DELETE\",\"ACTIVATE\"],\"subject\":\"guest\"}}";

        for (int i = 0; i < 100; ++i) {
            try {
                Activation activation = objectMapper.readValue(String.format(
                        activationBufferedScheduler,
                        UUID.randomUUID(),
                        "invoker" + random.ints(0, 3).findFirst().getAsInt(),
                        random.ints(0, 6).findFirst().getAsInt()),
                        Activation.class);
                data.add(activation);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }

        final IPolicy priorityQueueFIFIPolicy = PolicyFactory.createPolicy(Policy.PRIORITY_QUEUE_FIFO);
        final Queue<? extends ISchedulable> sortedQueue = priorityQueueFIFIPolicy.apply(data);
        sortedQueue.forEach(System.out::println);

        final Comparator<ISchedulable> priorityComparator = Comparator.comparing(ISchedulable::getPriority);
        final Comparator<ISchedulable> reversePriorityComparator = (s1, s2) ->
                Objects.requireNonNull(s2.getPriority()).compareTo(Objects.requireNonNull(s1.getPriority()));
        assertTrue(isSorted(sortedQueue, reversePriorityComparator));

        final ArrayDeque<? extends ISchedulable> arrayDequeSorted = new ArrayDeque<>(sortedQueue);
        assertTrue(arrayDequeSorted.peekFirst().getPriority() >= arrayDequeSorted.peekLast().getPriority());
    }

    private static boolean isSorted(@Nonnull Collection<? extends ISchedulable> schedulables,
                                    @Nonnull Comparator<ISchedulable> comparator) {
        if (schedulables.isEmpty() || schedulables.size() == 1) {
            return true;
        }

        final Iterator<? extends ISchedulable> iterator = schedulables.iterator();
        ISchedulable current, previous = iterator.next();
        while (iterator.hasNext()) {
            current = iterator.next();
            if (comparator.compare(previous, current) > 0) {
                return false;
            }
            previous = current;
        }
        return true;
    }

}