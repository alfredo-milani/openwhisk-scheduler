package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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

        Assertions.assertEquals(expectedActivation, deserializedActivation);
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

        Assertions.assertEquals(testHealthActivation, serializedActivation);
    }

}