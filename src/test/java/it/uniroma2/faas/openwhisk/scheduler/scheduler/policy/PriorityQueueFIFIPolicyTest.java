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

    @Test
    public void areActivationWithoutPriorityProcessedCorrectly() throws Exception {
        final Random random = new Random();
        final ObjectMapper objectMapper = new ObjectMapper();

        final Collection<Activation> data = new ArrayDeque<>();
        final String activationWithPriorityTemplate = "{\"action\": {\"name\": \"img_man\", \"path\": \"guest\", \"version\": \"0.0.2\"}, " +
                "\"activationId\": \"3d1f6dbf0510484c9f6dbf0510d84cb4\", \"blocking\": true, \"cause\": \"a6fd3a808e5a4e63bd3a808e5abe6392\", " +
                "\"content\": {\"$composer\": {\"openwhisk\": {\"ignore_certs\": true}, \"redis\": {\"uri\": \"redis://10.64.0.24:6379\"}}, " +
                "\"$scheduler\": {\"cmpLength\": 7, \"kind\": \"nodejs:10\", \"limits\": {\"concurrency\": 5, " +
                "\"memory\": 256, \"timeout\": 60000, \"userMemory\": 2048}, \"priority\": %d, \"target\": \"invoker0\"}}, \"initArgs\": [], " +
                "\"lockedArgs\": {}, \"revision\": \"2-a2eafd736df4f013107b161f69267890\", \"rootControllerIndex\": {\"asString\": \"0\", " +
                "\"instanceType\": \"controller\"}, \"transid\": [\"juI8HBMa0DNNfINnaqI1pZh1KYg65w4s\", 1629304522121, " +
                "[\"9xhK5ksnGllej1wwQnUhACJ6NKDFmYVu\", 1629304522117]], \"user\": {\"authkey\": {\"api_key\": " +
                "\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"}, " +
                "\"limits\": {}, \"namespace\": {\"name\": \"guest\", \"uuid\": \"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"}, " +
                "\"rights\": [\"READ\", \"PUT\", \"DELETE\", \"ACTIVATE\"], \"subject\": \"guest\"}}";
        final String activationWithoutPriorityTemplate = "{\"action\": {\"name\": \"img_man\", \"path\": \"guest\", " +
                "\"version\": \"0.0.2\"}, \"activationId\": \"3d1f6dbf0510484c9f6dbf0510d84cb4\", \"blocking\": true, " +
                "\"cause\": \"a6fd3a808e5a4e63bd3a808e5abe6392\", \"content\": {\"$composer\": {\"openwhisk\": " +
                "{\"ignore_certs\": true}, \"redis\": {\"uri\": \"redis://10.64.0.24:6379\"}}, \"$scheduler\": " +
                "{\"cmpLength\": 7, \"kind\": \"nodejs:10\", \"limits\": {\"concurrency\": 5, \"memory\": 256, " +
                "\"timeout\": 60000, \"userMemory\": 2048}, \"target\": \"invoker0\"}}, \"initArgs\": [], " +
                "\"lockedArgs\": {}, \"revision\": \"2-a2eafd736df4f013107b161f69267890\", \"rootControllerIndex\": " +
                "{\"asString\": \"0\", \"instanceType\": \"controller\"}, \"transid\": [\"juI8HBMa0DNNfINnaqI1pZh1KYg65w4s\", " +
                "1629304522121, [\"9xhK5ksnGllej1wwQnUhACJ6NKDFmYVu\", 1629304522117]], \"user\": {\"authkey\": " +
                "{\"api_key\": \"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"}, " +
                "\"limits\": {}, \"namespace\": {\"name\": \"guest\", \"uuid\": \"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"}, " +
                "\"rights\": [\"READ\", \"PUT\", \"DELETE\", \"ACTIVATE\"], \"subject\": \"guest\"}}";

        final Activation activationWithPriority = objectMapper.readValue(
                String.format(activationWithPriorityTemplate, 4),
                Activation.class
        );
        final Activation activationWithoutPriority = objectMapper.readValue(
                String.format(activationWithoutPriorityTemplate),
                Activation.class
        );
        data.add(activationWithPriority);
        data.add(activationWithoutPriority);

        final IPolicy priorityQueueFIFIPolicy = PolicyFactory.createPolicy(Policy.PRIORITY_QUEUE_FIFO);
        final Queue<? extends ISchedulable> sortedQueue = priorityQueueFIFIPolicy.apply(data);
        sortedQueue.forEach(System.out::println);
    }

}