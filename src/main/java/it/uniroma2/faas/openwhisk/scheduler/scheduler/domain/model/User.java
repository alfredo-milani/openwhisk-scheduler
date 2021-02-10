package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

public class User {

    /*
    "user":{
          "authkey":{
             "api_key":"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP"
          },
          "limits":{

          },
          "namespace":{
             "name":"guest",
             "uuid":"23bc46b1-71f6-4ed5-8c54-816aa4f8c502"
          },
          "rights":[
             "READ",
             "PUT",
             "DELETE",
             "ACTIVATE"
          ],
          "subject":"guest"
       }
     */

//    private final AuthKey authKey;
    private final Map<String, String> authKey;
    private final Map<String, String> limits;
    private final Namespace namespace;
    private final List<Right> rights;
    private final String subject;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public User(@JsonProperty("authkey") Map<String, String> authKey, @JsonProperty("limits") Map<String, String> limits,
                @JsonProperty("namespace") Namespace namespace, @JsonProperty("rights") List<Right> rights,
                @JsonProperty("subject") String subject) {
        this.authKey = authKey;
        this.limits = limits;
        this.namespace = namespace;
        this.rights = rights;
        this.subject = subject;
    }

    @JsonProperty("authkey")
    public Map<String, String> getAuthKey() {
        return authKey;
    }

    public Map<String, String> getLimits() {
        return limits;
    }

    public Namespace getNamespace() {
        return namespace;
    }

    public List<Right> getRights() {
        return rights;
    }

    public String getSubject() {
        return subject;
    }

    static class AuthKey {
        private final Map<String, String> apiKey;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public AuthKey(@JsonProperty("api") Map<String, String> authkey) {
            this.apiKey = authkey;
        }

        public Map<String, String> getApiKey() {
            return apiKey;
        }

        @Override
        public String toString() {
            return "AuthKey{" +
                    "apiKey=" + apiKey +
                    '}';
        }

    }

    static class Limits {
        private final Map<String, String> limits;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public Limits(@JsonProperty("limits") Map<String, String> limits) {
            this.limits = limits;
        }

        public Map<String, String> getLimits() {
            return limits;
        }

    }

    static class Namespace {
        private final String name;
        private final String uuid;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public Namespace(@JsonProperty("name") String name, @JsonProperty("uuid") String uuid) {
            this.name = name;
            this.uuid = uuid;
        }

        public String getName() {
            return name;
        }

        public String getUuid() {
            return uuid;
        }

        @Override
        public String toString() {
            return "Namespace{" +
                    "name='" + name + '\'' +
                    ", uuid='" + uuid + '\'' +
                    '}';
        }

    }

    // TODO: add others permissions (see on official site)
    enum Right {
        READ,
        PUT,
        DELETE,
        ACTIVATE;

        public @Nonnull Right from(@Nonnull String right) {
            for (Right r : Right.values()) {
                if (right.equalsIgnoreCase(r.name())) {
                    return r;
                }
            }
            throw new TypeNotPresentException(right, new Throwable("Selected right not yet implemented"));
        }
    }

    @Override
    public String toString() {
        return "User{" +
                "authKey=" + authKey +
                ", limits=" + limits +
                ", namespace=" + namespace +
                ", rights=" + rights +
                ", subject='" + subject + '\'' +
                '}';
    }

}