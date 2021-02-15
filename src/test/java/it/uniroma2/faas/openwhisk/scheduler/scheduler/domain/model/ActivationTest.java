package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.advanced.BufferedScheduler;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ActivationTest {

    /*
     * {
     *    "action":{
     *       "name":"invokerHealthTestAction0",
     *       "path":"whisk.system",
     *       "version":"0.0.1"
     *    },
     *    "activationId":"d26e7d5e9c0c4e9dae7d5e9c0c4e9d0c",
     *    "blocking":false,
     *    "content":{
     *       "$scheduler":{
     *          "target":"invoker0"
     *       }
     *    },
     *    "initArgs":[
     *
     *    ],
     *    "lockedArgs":{
     *
     *    },
     *    "revision":null,
     *    "rootControllerIndex":{
     *       "asString":"0",
     *       "instanceType":"controller"
     *    },
     *    "transid":[
     *       "sid_invokerHealth",
     *       1613230826360
     *    ],
     *    "user":{
     *       "authkey":{
     *          "api_key":"fd8a002b-b945-47d5-8a00-2bb945e7d584:PArwIfsNCvxFfvmZoZFopyy37s62Oi57TKO9Rr2IY1x9GNZIkyyMi6GFEvk6T7Rp"
     *       },
     *       "limits":{
     *
     *       },
     *       "namespace":{
     *          "name":"whisk.system",
     *          "uuid":"fd8a002b-b945-47d5-8a00-2bb945e7d584"
     *       },
     *       "rights":[
     *
     *       ],
     *       "subject":"whisk.system"
     *    }
     * }
     */
    @Test
    public void isTestHealthActivationDeserializedCorrectly() throws Exception {
        final String testHealthActivation = "{\"action\":{\"name\":\"invokerHealthTestAction0\",\"path\":\"whisk.system\",\"version\":\"0.0.1\"},\"activationId\":\"d26e7d5e9c0c4e9dae7d5e9c0c4e9d0c\",\"blocking\":false,\"content\":{\"$scheduler\":{\"target\":\"invoker0\"}},\"initArgs\":[],\"lockedArgs\":{},\"revision\":null,\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"sid_invokerHealth\",1613230826360],\"user\":{\"authkey\":{\"api_key\":\"fd8a002b-b945-47d5-8a00-2bb945e7d584:PArwIfsNCvxFfvmZoZFopyy37s62Oi57TKO9Rr2IY1x9GNZIkyyMi6GFEvk6T7Rp\"},\"limits\":{},\"namespace\":{\"name\":\"whisk.system\",\"uuid\":\"fd8a002b-b945-47d5-8a00-2bb945e7d584\"},\"rights\":[],\"subject\":\"whisk.system\"}}";
        System.out.println(testHealthActivation);
        final Activation deserializedActivation = new ObjectMapper().readValue(testHealthActivation, Activation.class);
        System.out.println(deserializedActivation);

        final Map<String, String> contentSchedulerMap = new HashMap<>(1) {{
           put("target", "invoker0");
        }};
        final Map<String, Object> contentMap = new HashMap<>(1) {{
            put("$scheduler", contentSchedulerMap);
        }};
        final Activation expectedActivation = new Activation(
                new Action("invokerHealthTestAction0", "whisk.system", "0.0.1"),
                "d26e7d5e9c0c4e9dae7d5e9c0c4e9d0c",
                false,
                null,
                contentMap,
                new ArrayList<>(),
                new HashMap<>(),
                null,
                new RootControllerIndex("0", RootControllerIndex.InstanceType.CONTROLLER),
                new TransId(new ArrayList<>(1) {{
                    add(new Transaction("sid_invokerHealth", 1613230826360L));
                }}),
                new User(new HashMap<>(1) {{
                    put("api_key", "fd8a002b-b945-47d5-8a00-2bb945e7d584:PArwIfsNCvxFfvmZoZFopyy37s62Oi57TKO9Rr2IY1x9GNZIkyyMi6GFEvk6T7Rp");
                }}, new HashMap<>(), new User.Namespace("whisk.system", "fd8a002b-b945-47d5-8a00-2bb945e7d584"), new ArrayList<>(), "whisk.system")
        );
        System.out.println(expectedActivation);

        assertEquals(expectedActivation, deserializedActivation);
    }

    @Test
    public void isTestHealthActivationSerializedCorrectly() throws Exception {
        final ObjectMapper objectMapper = new ObjectMapper();

        final Map<String, String> contentSchedulerMap = new HashMap<>(1) {{
            put("target", "invoker0");
        }};
        final Map<String, Object> contentMap = new HashMap<>(1) {{
            put("$scheduler", contentSchedulerMap);
        }};
        final Activation activation = new Activation(
                new Action("invokerHealthTestAction0", "whisk.system", "0.0.1"),
                "d26e7d5e9c0c4e9dae7d5e9c0c4e9d0c",
                false,
                null,
                contentMap,
                new ArrayList<>(),
                new HashMap<>(),
                null,
                new RootControllerIndex("0", RootControllerIndex.InstanceType.CONTROLLER),
                new TransId(new ArrayList<>(1) {{
                    add(new Transaction("sid_invokerHealth", 1613230826360L));
                }}),
                new User(new HashMap<>(1) {{
                    put("api_key", "fd8a002b-b945-47d5-8a00-2bb945e7d584:PArwIfsNCvxFfvmZoZFopyy37s62Oi57TKO9Rr2IY1x9GNZIkyyMi6GFEvk6T7Rp");
                }}, new HashMap<>(), new User.Namespace("whisk.system", "fd8a002b-b945-47d5-8a00-2bb945e7d584"), new ArrayList<>(), "whisk.system")
        );
        System.out.println(activation);
        final String serializedActivation = objectMapper.writeValueAsString(activation);
        System.out.println(serializedActivation);

        final String testHealthRecord = "{\"action\":{\"name\":\"invokerHealthTestAction0\",\"path\":\"whisk.system\",\"version\":\"0.0.1\"},\"activationId\":\"d26e7d5e9c0c4e9dae7d5e9c0c4e9d0c\",\"blocking\":false,\"content\":{\"$scheduler\":{\"target\":\"invoker0\"}},\"initArgs\":[],\"lockedArgs\":{},\"revision\":null,\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"sid_invokerHealth\",1613230826360],\"user\":{\"authkey\":{\"api_key\":\"fd8a002b-b945-47d5-8a00-2bb945e7d584:PArwIfsNCvxFfvmZoZFopyy37s62Oi57TKO9Rr2IY1x9GNZIkyyMi6GFEvk6T7Rp\"},\"limits\":{},\"namespace\":{\"name\":\"whisk.system\",\"uuid\":\"fd8a002b-b945-47d5-8a00-2bb945e7d584\"},\"rights\":[],\"subject\":\"whisk.system\"}}";
        final Activation testHealthActivation = objectMapper.readValue(testHealthRecord, Activation.class);
        final String deserializedActivation = objectMapper.writeValueAsString(testHealthActivation);
        System.out.println(testHealthActivation);

        assertEquals(deserializedActivation, serializedActivation);
    }

    @Test
    public void whenActivationIsInvokerHealthTestAction_thenTrue() throws Exception {
        final String recordOneDigit = "{\"action\":{\"name\":\"invokerHealthTestAction0\",\"path\":\"whisk.system\",\"version\":\"0.0.1\"},\"activationId\":\"b91ec105c18f4e8d9ec105c18f4e8d78\",\"blocking\":false,\"content\":{\"$scheduler\":{\"target\":\"invoker0\"}},\"initArgs\":[],\"lockedArgs\":{},\"revision\":null,\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"sid_invokerHealth\",1613297231269],\"user\":{\"authkey\":{\"api_key\":\"9327d432-cc30-472c-a7d4-32cc30872c45:oxk6Z7S5vxomcukeC20CKhIge2SxXxG9n7301QRJxjZVWr87WU3lSeb62E9En38g\"},\"limits\":{},\"namespace\":{\"name\":\"whisk.system\",\"uuid\":\"9327d432-cc30-472c-a7d4-32cc30872c45\"},\"rights\":[],\"subject\":\"whisk.system\"}}";
        final String recordTwoDigits = "{\"action\":{\"name\":\"invokerHealthTestAction12\",\"path\":\"whisk.system\",\"version\":\"0.0.1\"},\"activationId\":\"b91ec105c18f4e8d9ec105c18f4e8d78\",\"blocking\":false,\"content\":{\"$scheduler\":{\"target\":\"invoker0\"}},\"initArgs\":[],\"lockedArgs\":{},\"revision\":null,\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"sid_invokerHealth\",1613297231269],\"user\":{\"authkey\":{\"api_key\":\"9327d432-cc30-472c-a7d4-32cc30872c45:oxk6Z7S5vxomcukeC20CKhIge2SxXxG9n7301QRJxjZVWr87WU3lSeb62E9En38g\"},\"limits\":{},\"namespace\":{\"name\":\"whisk.system\",\"uuid\":\"9327d432-cc30-472c-a7d4-32cc30872c45\"},\"rights\":[],\"subject\":\"whisk.system\"}}";
        final String recordThreeDigits = "{\"action\":{\"name\":\"invokerHealthTestAction125\",\"path\":\"whisk.system\",\"version\":\"0.0.1\"},\"activationId\":\"b91ec105c18f4e8d9ec105c18f4e8d78\",\"blocking\":false,\"content\":{\"$scheduler\":{\"target\":\"invoker0\"}},\"initArgs\":[],\"lockedArgs\":{},\"revision\":null,\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"sid_invokerHealth\",1613297231269],\"user\":{\"authkey\":{\"api_key\":\"9327d432-cc30-472c-a7d4-32cc30872c45:oxk6Z7S5vxomcukeC20CKhIge2SxXxG9n7301QRJxjZVWr87WU3lSeb62E9En38g\"},\"limits\":{},\"namespace\":{\"name\":\"whisk.system\",\"uuid\":\"9327d432-cc30-472c-a7d4-32cc30872c45\"},\"rights\":[],\"subject\":\"whisk.system\"}}";
        final String recordNoDigits = "{\"action\":{\"name\":\"invokerHealthTestAction\",\"path\":\"whisk.system\",\"version\":\"0.0.1\"},\"activationId\":\"b91ec105c18f4e8d9ec105c18f4e8d78\",\"blocking\":false,\"content\":{\"$scheduler\":{\"target\":\"invoker0\"}},\"initArgs\":[],\"lockedArgs\":{},\"revision\":null,\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"sid_invokerHealth\",1613297231269],\"user\":{\"authkey\":{\"api_key\":\"9327d432-cc30-472c-a7d4-32cc30872c45:oxk6Z7S5vxomcukeC20CKhIge2SxXxG9n7301QRJxjZVWr87WU3lSeb62E9En38g\"},\"limits\":{},\"namespace\":{\"name\":\"whisk.system\",\"uuid\":\"9327d432-cc30-472c-a7d4-32cc30872c45\"},\"rights\":[],\"subject\":\"whisk.system\"}}";
        final String recordWrongAction = "{\"action\":{\"name\":\"invokerHealthTestAct\",\"path\":\"whisk.system\",\"version\":\"0.0.1\"},\"activationId\":\"b91ec105c18f4e8d9ec105c18f4e8d78\",\"blocking\":false,\"content\":{\"$scheduler\":{\"target\":\"invoker0\"}},\"initArgs\":[],\"lockedArgs\":{},\"revision\":null,\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"sid_invokerHealth\",1613297231269],\"user\":{\"authkey\":{\"api_key\":\"9327d432-cc30-472c-a7d4-32cc30872c45:oxk6Z7S5vxomcukeC20CKhIge2SxXxG9n7301QRJxjZVWr87WU3lSeb62E9En38g\"},\"limits\":{},\"namespace\":{\"name\":\"whisk.system\",\"uuid\":\"9327d432-cc30-472c-a7d4-32cc30872c45\"},\"rights\":[],\"subject\":\"whisk.system\"}}";
        final String recordWrongPath = "{\"action\":{\"name\":\"invokerHealthTestAction0\",\"path\":\"whisk.sys\",\"version\":\"0.0.1\"},\"activationId\":\"b91ec105c18f4e8d9ec105c18f4e8d78\",\"blocking\":false,\"content\":{\"$scheduler\":{\"target\":\"invoker0\"}},\"initArgs\":[],\"lockedArgs\":{},\"revision\":null,\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"sid_invokerHealth\",1613297231269],\"user\":{\"authkey\":{\"api_key\":\"9327d432-cc30-472c-a7d4-32cc30872c45:oxk6Z7S5vxomcukeC20CKhIge2SxXxG9n7301QRJxjZVWr87WU3lSeb62E9En38g\"},\"limits\":{},\"namespace\":{\"name\":\"whisk.system\",\"uuid\":\"9327d432-cc30-472c-a7d4-32cc30872c45\"},\"rights\":[],\"subject\":\"whisk.system\"}}";

        final ObjectMapper objectMapper = new ObjectMapper();
        final Activation activationOneDigit = objectMapper.readValue(recordOneDigit, Activation.class);
        final Activation activationTwoDigit = objectMapper.readValue(recordTwoDigits, Activation.class);
        final Activation activationThreeDigit = objectMapper.readValue(recordThreeDigits, Activation.class);
        final Activation activationNoDigit = objectMapper.readValue(recordNoDigits, Activation.class);
        final Activation activationWrongActivation = objectMapper.readValue(recordWrongAction, Activation.class);
        final Activation activationWrongPath = objectMapper.readValue(recordWrongPath, Activation.class);

        assertTrue(BufferedScheduler.isInvokerHealthTestAction(activationOneDigit.getAction()));
        assertTrue(BufferedScheduler.isInvokerHealthTestAction(activationTwoDigit.getAction()));
        assertTrue(BufferedScheduler.isInvokerHealthTestAction(activationThreeDigit.getAction()));
        assertFalse(BufferedScheduler.isInvokerHealthTestAction(activationNoDigit.getAction()));
        assertFalse(BufferedScheduler.isInvokerHealthTestAction(activationWrongActivation.getAction()));
        assertFalse(BufferedScheduler.isInvokerHealthTestAction(activationWrongPath.getAction()));
    }

    @Test
    public void whenActivationDeserialization_thenNoExceptionIsThrown() {
        final ObjectMapper objectMapper = new ObjectMapper();

        final String record = "{\"action\":{\"name\":\"test_annotations\",\"path\":\"guest\",\"version\":\"0.0.1\"},\"activationId\":\"0b307ee5a1304a57b07ee5a1300a57e7\",\"blocking\":true,\"content\":{\"key0\":\"value0\",\"key1\":\"value1\",\"key2\":\"value2\"},\"initArgs\":[],\"revision\":\"3-b3eeb1e516fd89366574c6051f024fa7\",\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"rxseFbQm3cy0QYASqVGkujB8lxEDE5Mu\",1609810319398],\"user\":{\"authkey\":{\"api_key\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"},\"limits\":{},\"namespace\":{\"name\":\"guest\",\"uuid\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"},\"rights\":[\"READ\",\"PUT\",\"DELETE\",\"ACTIVATE\"],\"subject\":\"guest\"}}";
        final String recordPriority = "{\"action\":{\"name\":\"test_annotations\",\"path\":\"guest\",\"version\":\"0.0.1\"},\"activationId\":\"0b307ee5a1304a57b07ee5a1300a57e7\",\"blocking\":true,\"content\":{\"key0\":\"value0\",\"key1\":\"value1\",\"key2\":\"value2\",\"$scheduler\":{\"target\":\"invoker2\"}},\"initArgs\":[],\"revision\":\"3-b3eeb1e516fd89366574c6051f024fa7\",\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"rxseFbQm3cy0QYASqVGkujB8lxEDE5Mu\",1609810319398],\"user\":{\"authkey\":{\"api_key\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"},\"limits\":{},\"namespace\":{\"name\":\"guest\",\"uuid\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"},\"rights\":[\"READ\",\"PUT\",\"DELETE\",\"ACTIVATE\"],\"subject\":\"guest\"}}";
        final String recordToTest = "{\"action\":{\"name\":\"invokerHealthTestAction0\",\"path\":\"whisk.system\",\"version\":\"0.0.1\"},\"activationId\":\"c87969bfe8cf4c2bb969bfe8cf3c2bf0\",\"blocking\":false,\"content\":{\"$scheduler\":{\"target\":\"invoker0\"}},\"initArgs\":[],\"lockedArgs\":{},\"revision\":null,\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"sid_invokerHealth\",1611036640735],\"user\":{\"authkey\":{\"api_key\":\"979552f2-6935-40aa-9552-f2693520aa71:nH3V9vtkRxy8Hcw1a88xHd7jotqefxjH5jTf8v5e1qnWXerfR67vvZyrj3EX9vu4\"},\"limits\":{},\"namespace\":{\"name\":\"whisk.system\",\"uuid\":\"979552f2-6935-40aa-9552-f2693520aa71\"},\"rights\":[],\"subject\":\"whisk.system\"}}";
        final String testHealthRecord = "{\"action\":{\"name\":\"invokerHealthTestAction0\",\"path\":\"whisk.system\",\"version\":\"0.0.1\"},\"activationId\":\"568a98b8da0548598a98b8da058859df\",\"blocking\":false,\"initArgs\":[],\"lockedArgs\":{},\"revision\":null,\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"sid_invokerHealth\",1612140411000],\"user\":{\"authkey\":{\"api_key\":\"07784fcd-9cb3-400a-b84f-cd9cb3f00a97:mMHbHXq0fXI3oTI5xQ5c9TtlayzGtxeXU6AEqYcBuZFzJWGUyrarMG8es68NANBW\"},\"limits\":{},\"namespace\":{\"name\":\"whisk.system\",\"uuid\":\"07784fcd-9cb3-400a-b84f-cd9cb3f00a97\"},\"rights\":[],\"subject\":\"whisk.system\"}}";

        assertDoesNotThrow(() -> System.out.println(objectMapper.readValue(record, Activation.class)));
        assertDoesNotThrow(() -> System.out.println(objectMapper.readValue(recordPriority, Activation.class)));
        assertDoesNotThrow(() -> System.out.println(objectMapper.readValue(recordToTest, Activation.class)));
        assertDoesNotThrow(() -> System.out.println(objectMapper.readValue(testHealthRecord, Activation.class)));
    }

    @Test
    public void whenActivationSerialization_thenNoExceptionIsThrown() {
        final ObjectMapper objectMapper = new ObjectMapper();

        final String record = "{\"action\":{\"name\":\"test_annotations\",\"path\":\"guest\",\"version\":\"0.0.1\"},\"activationId\":\"0b307ee5a1304a57b07ee5a1300a57e7\",\"blocking\":true,\"content\":{\"key0\":\"value0\",\"key1\":\"value1\",\"key2\":\"value2\"},\"initArgs\":[],\"revision\":\"3-b3eeb1e516fd89366574c6051f024fa7\",\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"rxseFbQm3cy0QYASqVGkujB8lxEDE5Mu\",1609810319398],\"user\":{\"authkey\":{\"api_key\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"},\"limits\":{},\"namespace\":{\"name\":\"guest\",\"uuid\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"},\"rights\":[\"READ\",\"PUT\",\"DELETE\",\"ACTIVATE\"],\"subject\":\"guest\"}}";
        final String recordNullContent = "{\"action\":{\"name\":\"test_annotations\",\"path\":\"guest\",\"version\":\"0.0.1\"},\"activationId\":\"0b307ee5a1304a57b07ee5a1300a57e7\",\"blocking\":true,\"initArgs\":[],\"revision\":\"3-b3eeb1e516fd89366574c6051f024fa7\",\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"rxseFbQm3cy0QYASqVGkujB8lxEDE5Mu\",1609810319398],\"user\":{\"authkey\":{\"api_key\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"},\"limits\":{},\"namespace\":{\"name\":\"guest\",\"uuid\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"},\"rights\":[\"READ\",\"PUT\",\"DELETE\",\"ACTIVATE\"],\"subject\":\"guest\"}}";
        final String recordPriority = "{\"action\":{\"name\":\"test_annotations\",\"path\":\"guest\",\"version\":\"0.0.1\"},\"activationId\":\"0b307ee5a1304a57b07ee5a1300a57e7\",\"blocking\":true,\"content\":{\"key0\":\"value0\",\"key1\":\"value1\",\"key2\":\"value2\",\"scheduler\":{\"target\":\"invoker2\"}},\"initArgs\":[],\"revision\":\"3-b3eeb1e516fd89366574c6051f024fa7\",\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"rxseFbQm3cy0QYASqVGkujB8lxEDE5Mu\",1609810319398],\"user\":{\"authkey\":{\"api_key\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"},\"limits\":{},\"namespace\":{\"name\":\"guest\",\"uuid\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"},\"rights\":[\"READ\",\"PUT\",\"DELETE\",\"ACTIVATE\"],\"subject\":\"guest\"}}";
        final String recordToTest = "{\"action\":{\"name\":\"invokerHealthTestAction0\",\"path\":\"whisk.system\",\"version\":\"0.0.1\"},\"activationId\":\"c87969bfe8cf4c2bb969bfe8cf3c2bf0\",\"blocking\":false,\"content\":{\"scheduler\":{\"target\":\"invoker0\"}},\"initArgs\":[],\"lockedArgs\":{},\"revision\":null,\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"sid_invokerHealth\",1611036640735],\"user\":{\"authkey\":{\"api_key\":\"979552f2-6935-40aa-9552-f2693520aa71:nH3V9vtkRxy8Hcw1a88xHd7jotqefxjH5jTf8v5e1qnWXerfR67vvZyrj3EX9vu4\"},\"limits\":{},\"namespace\":{\"name\":\"whisk.system\",\"uuid\":\"979552f2-6935-40aa-9552-f2693520aa71\"},\"rights\":[],\"subject\":\"whisk.system\"}}";
        final String recordOnlyTarget = "{\"action\":{\"name\":\"hello_py\",\"path\":\"guest\",\"version\":\"0.0.1\"},\"activationId\":\"1b319ed429234155b19ed42923a155cf\",\"blocking\":true,\"content\":{\"kTest\":\"vTest\",\"scheduler\":{\"target\":\"invoker0\",\"priority\":3}},\"initArgs\":[],\"lockedArgs\":{},\"revision\":\"1-ff07dcb3291545090f86e9fc1a01b5bf\",\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"4K6K9Uoz09O5ut42VEiFI9zrOAiJX8oq\",1611192819095],\"user\":{\"authkey\":{\"api_key\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"},\"limits\":{},\"namespace\":{\"name\":\"guest\",\"uuid\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"},\"rights\":[\"READ\",\"PUT\",\"DELETE\",\"ACTIVATE\"],\"subject\":\"guest\"}}";

        assertDoesNotThrow(() -> {
            final Activation activation = objectMapper.readValue(record, Activation.class);
            System.out.println(objectMapper.writeValueAsString(activation));
        });
        assertDoesNotThrow(() -> {
            final Activation activation = objectMapper.readValue(recordNullContent, Activation.class);
            System.out.println(objectMapper.writeValueAsString(activation));
        });
        assertDoesNotThrow(() -> {
            final Activation activation = objectMapper.readValue(recordPriority, Activation.class);
            System.out.println(objectMapper.writeValueAsString(activation));
        });
        assertDoesNotThrow(() -> {
            final Activation activation = objectMapper.readValue(recordToTest, Activation.class);
            System.out.println(objectMapper.writeValueAsString(activation));
        });
        assertDoesNotThrow(() -> {
            final Activation activation = objectMapper.readValue(recordOnlyTarget, Activation.class);
            System.out.println(objectMapper.writeValueAsString(activation));
        });
    }

    @Test
    public void whenActivationWithNewPriorityCreated_thenNewPriorityUpdatedCorrectly() throws Exception {
        final ObjectMapper objectMapper = new ObjectMapper();

        final String record = "{\"action\":{\"name\":\"fn2\",\"path\":\"guest\",\"version\":\"0.0.1\"},\"activationId\":\"93a8dd0750934856a8dd0750935856ba\",\"blocking\":true,\"cause\":\"7402792454a04aca82792454a09aca14\",\"content\":{\"$scheduler\":{\"duration\":10,\"limits\":{\"concurrency\":3,\"memory\":256,\"timeout\":60000,\"userMemory\":2048},\"overload\":false,\"priority\":0,\"target\":\"invoker0\"},\"extras\":{\"cmd\":\"Sleep executed for 5 s.\",\"fn_name\":\"/guest/fn1\"},\"message\":\"Hello Kira!\",\"sleep_time\":5},\"initArgs\":[],\"lockedArgs\":{},\"revision\":\"1-f5098d6fdb4e59dd7348545470ab110f\",\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"WSfYUfdxl3hm7HHsmykAp1nrFwGKGC8l\",1613391003698,[\"MDEXs94DFVXmCKZmRgdIFSA7Zcsp2x0j\",1613391003260,[\"R9O0tKt4PnXSUz9Qu97g6thHCSP6jgrw\",1613390993698,[\"fvxblQafi4egIwb6Hw4KWokFLCbZh305\",1613390988735,[\"P8XxEP8sFUs5g9Gf8pykM4SjpGp6AiQA\",1613390988230]]]]],\"user\":{\"authkey\":{\"api_key\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"},\"limits\":{},\"namespace\":{\"name\":\"guest\",\"uuid\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"},\"rights\":[\"READ\",\"PUT\",\"DELETE\",\"ACTIVATE\"],\"subject\":\"guest\"}}";

        final Activation activation = objectMapper.readValue(record, Activation.class);
        final int newPriority = 5;
        final Activation activationWithNewPriority = activation.with(newPriority);

        assertEquals(newPriority, activationWithNewPriority.getPriority());
    }

    @Test
    public void givenNewPriority_whenActivationSerialization_thenNoExceptionIsThrown() throws Exception {
        final ObjectMapper objectMapper = new ObjectMapper();

        final String record = "{\"action\":{\"name\":\"test_annotations\",\"path\":\"guest\",\"version\":\"0.0.1\"},\"activationId\":\"0b307ee5a1304a57b07ee5a1300a57e7\",\"blocking\":true,\"content\":{\"key0\":\"value0\",\"key1\":\"value1\",\"key2\":\"value2\"},\"initArgs\":[],\"revision\":\"3-b3eeb1e516fd89366574c6051f024fa7\",\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"rxseFbQm3cy0QYASqVGkujB8lxEDE5Mu\",1609810319398],\"user\":{\"authkey\":{\"api_key\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"},\"limits\":{},\"namespace\":{\"name\":\"guest\",\"uuid\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"},\"rights\":[\"READ\",\"PUT\",\"DELETE\",\"ACTIVATE\"],\"subject\":\"guest\"}}";
        final Activation activationRecord = objectMapper.readValue(record, Activation.class);
        final String recordPriority = "{\"action\":{\"name\":\"test_annotations\",\"path\":\"guest\",\"version\":\"0.0.1\"},\"activationId\":\"0b307ee5a1304a57b07ee5a1300a57e7\",\"blocking\":true,\"content\":{\"key0\":\"value0\",\"key1\":\"value1\",\"key2\":\"value2\",\"$scheduler\":{\"target\":\"invoker2\"}},\"initArgs\":[],\"revision\":\"3-b3eeb1e516fd89366574c6051f024fa7\",\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"rxseFbQm3cy0QYASqVGkujB8lxEDE5Mu\",1609810319398],\"user\":{\"authkey\":{\"api_key\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"},\"limits\":{},\"namespace\":{\"name\":\"guest\",\"uuid\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"},\"rights\":[\"READ\",\"PUT\",\"DELETE\",\"ACTIVATE\"],\"subject\":\"guest\"}}";
        final Activation activationPriority = objectMapper.readValue(recordPriority, Activation.class);
        final String recordToTest = "{\"action\":{\"name\":\"invokerHealthTestAction0\",\"path\":\"whisk.system\",\"version\":\"0.0.1\"},\"activationId\":\"c87969bfe8cf4c2bb969bfe8cf3c2bf0\",\"blocking\":false,\"content\":{\"$scheduler\":{\"target\":\"invoker0\"}},\"initArgs\":[],\"lockedArgs\":{},\"revision\":null,\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"sid_invokerHealth\",1611036640735],\"user\":{\"authkey\":{\"api_key\":\"979552f2-6935-40aa-9552-f2693520aa71:nH3V9vtkRxy8Hcw1a88xHd7jotqefxjH5jTf8v5e1qnWXerfR67vvZyrj3EX9vu4\"},\"limits\":{},\"namespace\":{\"name\":\"whisk.system\",\"uuid\":\"979552f2-6935-40aa-9552-f2693520aa71\"},\"rights\":[],\"subject\":\"whisk.system\"}}";
        final Activation toTestActivation = objectMapper.readValue(recordToTest, Activation.class);
        final String testHealthRecord = "{\"action\":{\"name\":\"invokerHealthTestAction0\",\"path\":\"whisk.system\",\"version\":\"0.0.1\"},\"activationId\":\"568a98b8da0548598a98b8da058859df\",\"blocking\":false,\"initArgs\":[],\"lockedArgs\":{},\"revision\":null,\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"sid_invokerHealth\",1612140411000],\"user\":{\"authkey\":{\"api_key\":\"07784fcd-9cb3-400a-b84f-cd9cb3f00a97:mMHbHXq0fXI3oTI5xQ5c9TtlayzGtxeXU6AEqYcBuZFzJWGUyrarMG8es68NANBW\"},\"limits\":{},\"namespace\":{\"name\":\"whisk.system\",\"uuid\":\"07784fcd-9cb3-400a-b84f-cd9cb3f00a97\"},\"rights\":[],\"subject\":\"whisk.system\"}}";
        final Activation testHealthActivation = objectMapper.readValue(testHealthRecord, Activation.class);

        assertDoesNotThrow(() -> System.out.println(objectMapper.writeValueAsString(activationRecord.with(0))));
        assertDoesNotThrow(() -> System.out.println(objectMapper.writeValueAsString(activationPriority.with(0))));
        assertDoesNotThrow(() -> System.out.println(objectMapper.writeValueAsString(toTestActivation.with(0))));
        assertDoesNotThrow(() -> System.out.println(objectMapper.writeValueAsString(testHealthActivation.with(0))));

        assertDoesNotThrow(() -> System.out.println(objectMapper.writeValueAsString(activationRecord.with(5))));
        assertDoesNotThrow(() -> System.out.println(objectMapper.writeValueAsString(activationPriority.with(5))));
        assertDoesNotThrow(() -> System.out.println(objectMapper.writeValueAsString(toTestActivation.with(5))));
        assertDoesNotThrow(() -> System.out.println(objectMapper.writeValueAsString(testHealthActivation.with(5))));

        assertThrows(IllegalArgumentException.class,
                () -> System.out.println(objectMapper.writeValueAsString(activationRecord.with(-1))));
        assertThrows(IllegalArgumentException.class,
                () -> System.out.println(objectMapper.writeValueAsString(activationPriority.with(-1))));
        assertThrows(IllegalArgumentException.class,
                () -> System.out.println(objectMapper.writeValueAsString(toTestActivation.with(-1))));
        assertThrows(IllegalArgumentException.class,
                () -> System.out.println(objectMapper.writeValueAsString(testHealthActivation.with(-1))));
    }

    @Test
    public void givenTerminationSchedulingTimestamp_whenActivationSerialization_thenNoExceptionIsThrown() throws Exception {
        final ObjectMapper objectMapper = new ObjectMapper();

        final String record = "{\"action\":{\"name\":\"test_annotations\",\"path\":\"guest\",\"version\":\"0.0.1\"},\"activationId\":\"0b307ee5a1304a57b07ee5a1300a57e7\",\"blocking\":true,\"content\":{\"key0\":\"value0\",\"key1\":\"value1\",\"key2\":\"value2\"},\"initArgs\":[],\"revision\":\"3-b3eeb1e516fd89366574c6051f024fa7\",\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"rxseFbQm3cy0QYASqVGkujB8lxEDE5Mu\",1609810319398],\"user\":{\"authkey\":{\"api_key\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"},\"limits\":{},\"namespace\":{\"name\":\"guest\",\"uuid\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"},\"rights\":[\"READ\",\"PUT\",\"DELETE\",\"ACTIVATE\"],\"subject\":\"guest\"}}";
        final Activation activationRecord = objectMapper.readValue(record, Activation.class);

        final long creationTimestamp = activationRecord.getCreationTimestamp();
        final long schedulingDuration = 100L;
        final long schedulingTermination = creationTimestamp + schedulingDuration;

        System.out.println(activationRecord.with(schedulingTermination));
    }

}