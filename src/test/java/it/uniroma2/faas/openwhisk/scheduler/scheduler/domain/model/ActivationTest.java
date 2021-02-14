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
        final String serializedActivation = new ObjectMapper().writeValueAsString(activation);
        System.out.println(serializedActivation);

        final String testHealthActivation = "{\"action\":{\"name\":\"invokerHealthTestAction0\",\"path\":\"whisk.system\",\"version\":\"0.0.1\"},\"activationId\":\"d26e7d5e9c0c4e9dae7d5e9c0c4e9d0c\",\"blocking\":false,\"content\":{\"$scheduler\":{\"target\":\"invoker0\"}},\"initArgs\":[],\"lockedArgs\":{},\"revision\":null,\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"sid_invokerHealth\",1613230826360],\"user\":{\"authkey\":{\"api_key\":\"fd8a002b-b945-47d5-8a00-2bb945e7d584:PArwIfsNCvxFfvmZoZFopyy37s62Oi57TKO9Rr2IY1x9GNZIkyyMi6GFEvk6T7Rp\"},\"limits\":{},\"namespace\":{\"name\":\"whisk.system\",\"uuid\":\"fd8a002b-b945-47d5-8a00-2bb945e7d584\"},\"rights\":[],\"subject\":\"whisk.system\"}}";
        System.out.println(testHealthActivation);

        assertEquals(testHealthActivation, serializedActivation);
    }

    @Test
    public void whenActivationIsIvnokerHealthTestAction_thenTrue() throws Exception {
        final String recordOneDigit = "{\"action\":{\"name\":\"invokerHealthTestAction0\",\"path\":\"whisk.system\",\"version\":\"0.0.1\"},\"activationId\":\"b91ec105c18f4e8d9ec105c18f4e8d78\",\"blocking\":false,\"content\":{\"$scheduler\":{\"target\":\"invoker0\"}},\"initArgs\":[],\"lockedArgs\":{},\"revision\":null,\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"sid_invokerHealth\",1613297231269],\"user\":{\"authkey\":{\"api_key\":\"9327d432-cc30-472c-a7d4-32cc30872c45:oxk6Z7S5vxomcukeC20CKhIge2SxXxG9n7301QRJxjZVWr87WU3lSeb62E9En38g\"},\"limits\":{},\"namespace\":{\"name\":\"whisk.system\",\"uuid\":\"9327d432-cc30-472c-a7d4-32cc30872c45\"},\"rights\":[],\"subject\":\"whisk.system\"}}";
        final String recordTwoDigits = "{\"action\":{\"name\":\"invokerHealthTestAction12\",\"path\":\"whisk.system\",\"version\":\"0.0.1\"},\"activationId\":\"b91ec105c18f4e8d9ec105c18f4e8d78\",\"blocking\":false,\"content\":{\"$scheduler\":{\"target\":\"invoker0\"}},\"initArgs\":[],\"lockedArgs\":{},\"revision\":null,\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"sid_invokerHealth\",1613297231269],\"user\":{\"authkey\":{\"api_key\":\"9327d432-cc30-472c-a7d4-32cc30872c45:oxk6Z7S5vxomcukeC20CKhIge2SxXxG9n7301QRJxjZVWr87WU3lSeb62E9En38g\"},\"limits\":{},\"namespace\":{\"name\":\"whisk.system\",\"uuid\":\"9327d432-cc30-472c-a7d4-32cc30872c45\"},\"rights\":[],\"subject\":\"whisk.system\"}}";
        final String recordNoDigits = "{\"action\":{\"name\":\"invokerHealthTestAction\",\"path\":\"whisk.system\",\"version\":\"0.0.1\"},\"activationId\":\"b91ec105c18f4e8d9ec105c18f4e8d78\",\"blocking\":false,\"content\":{\"$scheduler\":{\"target\":\"invoker0\"}},\"initArgs\":[],\"lockedArgs\":{},\"revision\":null,\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"sid_invokerHealth\",1613297231269],\"user\":{\"authkey\":{\"api_key\":\"9327d432-cc30-472c-a7d4-32cc30872c45:oxk6Z7S5vxomcukeC20CKhIge2SxXxG9n7301QRJxjZVWr87WU3lSeb62E9En38g\"},\"limits\":{},\"namespace\":{\"name\":\"whisk.system\",\"uuid\":\"9327d432-cc30-472c-a7d4-32cc30872c45\"},\"rights\":[],\"subject\":\"whisk.system\"}}";
        final String recordWrongAction = "{\"action\":{\"name\":\"invokerHealthTestAct\",\"path\":\"whisk.system\",\"version\":\"0.0.1\"},\"activationId\":\"b91ec105c18f4e8d9ec105c18f4e8d78\",\"blocking\":false,\"content\":{\"$scheduler\":{\"target\":\"invoker0\"}},\"initArgs\":[],\"lockedArgs\":{},\"revision\":null,\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"sid_invokerHealth\",1613297231269],\"user\":{\"authkey\":{\"api_key\":\"9327d432-cc30-472c-a7d4-32cc30872c45:oxk6Z7S5vxomcukeC20CKhIge2SxXxG9n7301QRJxjZVWr87WU3lSeb62E9En38g\"},\"limits\":{},\"namespace\":{\"name\":\"whisk.system\",\"uuid\":\"9327d432-cc30-472c-a7d4-32cc30872c45\"},\"rights\":[],\"subject\":\"whisk.system\"}}";
        final String recordWrongPath = "{\"action\":{\"name\":\"invokerHealthTestAction0\",\"path\":\"whisk.sys\",\"version\":\"0.0.1\"},\"activationId\":\"b91ec105c18f4e8d9ec105c18f4e8d78\",\"blocking\":false,\"content\":{\"$scheduler\":{\"target\":\"invoker0\"}},\"initArgs\":[],\"lockedArgs\":{},\"revision\":null,\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"sid_invokerHealth\",1613297231269],\"user\":{\"authkey\":{\"api_key\":\"9327d432-cc30-472c-a7d4-32cc30872c45:oxk6Z7S5vxomcukeC20CKhIge2SxXxG9n7301QRJxjZVWr87WU3lSeb62E9En38g\"},\"limits\":{},\"namespace\":{\"name\":\"whisk.system\",\"uuid\":\"9327d432-cc30-472c-a7d4-32cc30872c45\"},\"rights\":[],\"subject\":\"whisk.system\"}}";

        final Activation activationOneDigit = new ObjectMapper().readValue(recordOneDigit, Activation.class);
        final Activation activationTwoDigit = new ObjectMapper().readValue(recordTwoDigits, Activation.class);
        final Activation activationNoDigit = new ObjectMapper().readValue(recordNoDigits, Activation.class);
        final Activation activationWrongActivation = new ObjectMapper().readValue(recordWrongAction, Activation.class);
        final Activation activationWrongPath = new ObjectMapper().readValue(recordWrongPath, Activation.class);

        assertTrue(BufferedScheduler.isInvokerHealthTestAction(activationOneDigit.getAction()));
        assertTrue(BufferedScheduler.isInvokerHealthTestAction(activationTwoDigit.getAction()));
        assertFalse(BufferedScheduler.isInvokerHealthTestAction(activationNoDigit.getAction()));
        assertFalse(BufferedScheduler.isInvokerHealthTestAction(activationWrongActivation.getAction()));
        assertFalse(BufferedScheduler.isInvokerHealthTestAction(activationWrongPath.getAction()));
    }

}