package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.scheduler.IBufferizable;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.scheduler.advanced.ITraceable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

// @JsonInclude(JsonInclude.Include.NON_NULL)
public final class Activation implements ITraceable, IBufferizable {

    /*
     * {
     *   "action": {
     *     "name": "invokerHealthTestAction0",
     *     "path": "whisk.system",
     *     "version": "0.0.1"
     *   },
     *   "activationId": "f16f3319467741dfaf3319467741df55",
     *   "blocking": false,
     *   "initArgs": [],
     *   "lockedArgs": {},
     *   "revision": null,
     *   "rootControllerIndex": {
     *     "asString": "0",
     *     "instanceType": "controller"
     *   },
     *   "transid": [
     *     "sid_invokerHealth",
     *     1610726319878
     *   ],
     *   "user": {
     *     "authkey": {
     *       "api_key": "0e31a905-7de9-4345-b1a9-057de98345be:yvOm0CJajJEi82htxUXva5TKsZDcDsyZQRkV1GtawZNoHzN5ddQCI0VOldYyUuZZ"
     *     },
     *     "limits": {},
     *     "namespace": {
     *       "name": "whisk.system",
     *       "uuid": "0e31a905-7de9-4345-b1a9-057de98345be"
     *     },
     *     "rights": [],
     *     "subject": "whisk.system"
     *   }
     * }
     */

    /*
     * {
     *   "action": {
     *     "name": "test_annotations",
     *     "path": "guest",
     *     "version": "0.0.1"
     *   },
     *   "activationId": "0b307ee5a1304a57b07ee5a1300a57e7",
     *   "blocking": true,
     *   "content": {
     *     "key0": "value0",
     *     "key1": "value1",
     *     "key2": "value2",
     *     "$scheduler": {
     *       "target": "invoker0",
     *       "priority": 0
     *     }
     *   },
     *   "initArgs": [],
     *   "revision": "3-b3eeb1e516fd89366574c6051f024fa7",
     *   "rootControllerIndex": {
     *     "asString": "0",
     *     "instanceType": "controller"
     *   },
     *   "transid": [
     *     "rxseFbQm3cy0QYASqVGkujB8lxEDE5Mu",
     *     1609810319398
     *   ],
     *   "user": {
     *     "authkey": {
     *       "api_key": "23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP"
     *     },
     *     "limits": {},
     *     "namespace": {
     *       "name": "guest",
     *       "uuid": "23bc46b1-71f6-4ed5-8c54-816aa4f8c502"
     *     },
     *     "rights": [
     *       "READ",
     *       "PUT",
     *       "DELETE",
     *       "ACTIVATE"
     *     ],
     *     "subject": "guest"
     *   }
     * }
     */

    /*
     * {
     *        "action":{
     *           "name":"cmp",
     *           "path":"guest",
     *           "version":"0.0.2"
     *        },
     *        "activationId":"841e3f99f40346429e3f99f40396428f",
     *        "blocking":true,
     *        "cause":"7902f85843f349df82f85843f309dfb0",
     *        "content":{
     *           "sleep_time":15,
     *           "user":"Kira"
     *        },
     *        "initArgs":[
     *
     *        ],
     *        "revision":"2-ec8832836d2a16234f464c9445e34587",
     *        "rootControllerIndex":{
     *           "asString":"0",
     *           "instanceType":"controller"
     *        },
     *        "transid":[
     *           "2rkHZR02ErZB6kol4tl5LN4oxiHB5VH8",
     *           1609870676410,
     *           [
     *              "iRFErOK5Hflz8qduy49vBKWWMFTG44IW",
     *              1609870676273
     *           ]
     *        ],
     *        "user":{
     *           "authkey":{
     *              "api_key":"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP"
     *           },
     *           "limits":{
     *
     *           },
     *           "namespace":{
     *              "name":"guest",
     *              "uuid":"23bc46b1-71f6-4ed5-8c54-816aa4f8c502"
     *           },
     *           "rights":[
     *              "READ",
     *              "PUT",
     *              "DELETE",
     *              "ACTIVATE"
     *           ],
     *           "subject":"guest"
     *        }
     *     }
     */

    /*
     * {
     *   "action": {
     *     "name": "fn1",
     *     "path": "guest",
     *     "version": "0.0.2"
     *   },
     *   "activationId": "7408d52ef57d458788d52ef57d5587b4",
     *   "blocking": true,
     *   "cause": "7902f85843f349df82f85843f309dfb0",
     *   "content": {
     *     "sleep_time": 15,
     *     "user": "Kira"
     *   },
     *   "initArgs": [],
     *   "revision": "2-2bd48aaaf6e1721bca963e680eee313e",
     *   "rootControllerIndex": {
     *     "asString": "0",
     *     "instanceType": "controller"
     *   },
     *   "transid": [
     *     "bxytKe9Qk3EYVEYQnpHs02NDFo5eKAd3",
     *     1609870677855,
     *     [
     *       "2rkHZR02ErZB6kol4tl5LN4oxiHB5VH8",
     *       1609870676410,
     *       [
     *         "iRFErOK5Hflz8qduy49vBKWWMFTG44IW",
     *         1609870676273
     *       ]
     *     ]
     *   ],
     *   "user": {
     *     "authkey": {
     *       "api_key": "23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP"
     *     },
     *     "limits": {},
     *     "namespace": {
     *       "name": "guest",
     *       "uuid": "23bc46b1-71f6-4ed5-8c54-816aa4f8c502"
     *     },
     *     "rights": [
     *       "READ",
     *       "PUT",
     *       "DELETE",
     *       "ACTIVATE"
     *     ],
     *     "subject": "guest"
     *   }
     * }
     */

    public static final String K_SCHEDULER = "$scheduler";
    public static final String K_SCHEDULER_TARGET = "target";
    public static final String K_SCHEDULER_PRIORITY = "priority";
    public static final String K_SCHEDULER_DURATION = "duration";
    public static final String K_SCHEDULER_OVERLOAD = "overload";
    public static final String K_SCHEDULER_KIND = "kind";
    public static final String K_SCHEDULER_LIMITS = "limits";
    public static final String K_SCHEDULER_LIMITS_CONCURRENCY = "concurrency";
    public static final String K_SCHEDULER_LIMITS_MEMORY = "memory";
    public static final String K_SCHEDULER_LIMITS_TIME = "timeout";
    public static final String K_SCHEDULER_LIMITS_USER_MEMORY = "userMemory";

    private final Action action;
    private final String activationId;
    private final Boolean blocking;
    private final String cause;
    private final Map<String, Object> content;
    private final List<String> initArgs;
    private final Map<String, String> lockedArgs;
    private final String revision;
    private final RootControllerIndex rootControllerIndex;
    private final TransId transId;
    private final User user;

    // this value will be the topic name where activation will be sent
    private final String targetInvoker;
    // using Integer priority, there is not an upper bound to max priority
    private final Integer priority;
    // at creation, contains creation timestamp
    private final long creationTimestamp;
    // indicate invoker overloading
    private final Boolean overload;
    // action runtime description
    private final String kind;
    // limits for current activation
    private final Long concurrencyLimit;
    private final Long memoryLimit;
    private final Long timeLimit;
    private final Long userMemory;

    @SuppressWarnings("unchecked")
    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public Activation(@JsonProperty("action") Action action, @JsonProperty("activationId") String activationId,
                      @JsonProperty("blocking") Boolean blocking, @JsonProperty("cause") String cause,
                      @JsonProperty("content") Map<String, Object> content, @JsonProperty("initArgs") List<String> initArgs,
                      @JsonProperty("lockedArgs") Map<String, String> lockedArgs, @JsonProperty("revision") String revision,
                      @JsonProperty("rootControllerIndex") RootControllerIndex rootControllerIndex,
                      @JsonProperty("transid") TransId transId, @JsonProperty("user") User user) {
        this.action = action;
        this.activationId = activationId;
        this.blocking = blocking;
        this.cause = cause;
        this.content = content;
        this.initArgs = initArgs;
        this.lockedArgs = lockedArgs;
        this.revision = revision;
        this.rootControllerIndex = rootControllerIndex;
        this.transId = transId;
        this.user = user;

        String targetInvoker = null;
        Integer priority = null;
        Boolean overload = null;
        String kind = null;
        Long concurrencyLimit = null;
        Long memoryLimit = null;
        Long timeLimit = null;
        Long userMemory = null;
        // unpack content
        if (this.content != null) {
            // Note: K_SCHEDULER object could be removed from content (this.content.remove(K_SCHEDULER);
            //   it is maintained to retrieve data for testing purpose
            Map<String, Object> scheduler = (Map<String, Object>) this.content.get(K_SCHEDULER);
            if (scheduler != null) {
                targetInvoker = (String) scheduler.get(K_SCHEDULER_TARGET);
                Number priorityNumber = (Number) scheduler.get(K_SCHEDULER_PRIORITY);
                if (priorityNumber != null) priority = priorityNumber.intValue();
                overload = (Boolean) scheduler.get(K_SCHEDULER_OVERLOAD);
                kind = (String) scheduler.get(K_SCHEDULER_KIND);
                Map<String, Object> limits = (Map<String, Object>) scheduler.get(K_SCHEDULER_LIMITS);
                if (limits != null) {
                    Number concurrencyNumber = (Number) limits.get(K_SCHEDULER_LIMITS_CONCURRENCY);
                    if (concurrencyNumber != null) concurrencyLimit = concurrencyNumber.longValue();
                    Number memoryLimitNumber = (Number) limits.get(K_SCHEDULER_LIMITS_MEMORY);
                    if (memoryLimitNumber != null) memoryLimit = memoryLimitNumber.longValue();
                    Number timeLimitNumber = (Number) limits.get(K_SCHEDULER_LIMITS_TIME);
                    if (timeLimitNumber != null) timeLimit = timeLimitNumber.longValue();
                    Number userMemoryNumber = (Number) limits.get(K_SCHEDULER_LIMITS_USER_MEMORY);
                    if (userMemoryNumber != null) userMemory = userMemoryNumber.longValue();
                }
            }
        }
        this.targetInvoker = targetInvoker;
        this.priority = priority;
        this.creationTimestamp = Instant.now().toEpochMilli();
        this.overload = overload;
        this.kind = kind;
        this.concurrencyLimit = concurrencyLimit;
        this.memoryLimit = memoryLimit;
        this.timeLimit = timeLimit;
        this.userMemory = userMemory;
    }

    // TODO - implement deep copy
    // Current implementation provides a shallow copy
    @SuppressWarnings("unchecked")
    @Override
    public @Nonnull Activation with(int priority) {
        checkArgument(priority >= 0, "Priority must be >= 0.");

        Map<String, Object> content;
        if (this.content == null) {
            content = new HashMap<>();
        } else {
            content = new HashMap<>(this.content);
        }
        content.putIfAbsent(K_SCHEDULER, new HashMap<>());
        final Map<String, Object> scheduler = (Map<String, Object>) content.get(K_SCHEDULER);
        scheduler.put(K_SCHEDULER_PRIORITY, priority);

        return new Activation(
                this.getAction(), this.getActivationId(),
                this.isBlocking(), this.getCause(),
                content, this.getInitArgs(),
                this.getLockedArgs(), this.getRevision(),
                this.getRootControllerIndex(), this.getTransId(),
                this.getUser()
        );
    }

    // TODO - implement deep copy
    // Current implementation provides a shallow copy
    @SuppressWarnings("unchecked")
    @Override
    public @Nonnull Activation with(long schedulingTermination) {
        checkArgument(schedulingTermination >= 0, "Scheduling duration must be >= 0.");

        Map<String, Object> content;
        if (this.content == null) {
            content = new HashMap<>();
        } else {
            content = new HashMap<>(this.content);
        }
        content.putIfAbsent(K_SCHEDULER, new HashMap<>());
        final Map<String, Object> scheduler = (Map<String, Object>) content.get(K_SCHEDULER);
        scheduler.putIfAbsent(K_SCHEDULER_DURATION, schedulingTermination - creationTimestamp);

        return new Activation(
                this.getAction(), this.getActivationId(),
                this.isBlocking(), this.getCause(),
                content, this.getInitArgs(),
                this.getLockedArgs(), this.getRevision(),
                this.getRootControllerIndex(), this.getTransId(),
                this.getUser()
        );
    }

    // TODO - implement deep copy
    // Current implementation provides a shallow copy
    @SuppressWarnings("unchecked")
    @Override
    public @Nonnull Activation with(@Nonnull String invokerTarget) {
        checkNotNull(invokerTarget, "Invoker target can not be null.");

        Map<String, Object> content;
        if (this.content == null) {
            content = new HashMap<>();
        } else {
            content = new HashMap<>(this.content);
        }
        content.putIfAbsent(K_SCHEDULER, new HashMap<>());
        final Map<String, Object> scheduler = (Map<String, Object>) content.get(K_SCHEDULER);
        scheduler.putIfAbsent(K_SCHEDULER_TARGET, invokerTarget);

        return new Activation(
                this.getAction(), this.getActivationId(),
                this.isBlocking(), this.getCause(),
                content, this.getInitArgs(),
                this.getLockedArgs(), this.getRevision(),
                this.getRootControllerIndex(), this.getTransId(),
                this.getUser()
        );
    }

    @Override
    public Action getAction() {
        return action;
    }

    @Nonnull
    public String getActivationId() {
        return activationId;
    }

    public boolean isBlocking() {
        return blocking;
    }

    @Override
    public @Nullable String getCause() {
        return cause;
    }

    public Map<String, Object> getContent() {
        return content;
    }

    public List<String> getInitArgs() {
        return initArgs;
    }

    public Map<String, String> getLockedArgs() {
        return lockedArgs;
    }

    public String getRevision() {
        return revision;
    }

    @Override
    public RootControllerIndex getRootControllerIndex() {
        return rootControllerIndex;
    }

    @JsonProperty("transid")
    public TransId getTransId() {
        return transId;
    }

    public User getUser() {
        return user;
    }

    @JsonIgnore
    @Override
    public String getTargetInvoker() {
        return targetInvoker;
    }

    @JsonIgnore
    @Override
    public @Nullable Integer getPriority() {
        return priority;
    }

    @JsonIgnore
    public long getCreationTimestamp() {
        return creationTimestamp;
    }

    @JsonIgnore
    @Override
    public @Nullable Boolean getOverload() {
        return overload;
    }

    @JsonIgnore
    @Override
    public @Nullable String getKind() {
        return kind;
    }

    @JsonIgnore
    @Override
    public Long getConcurrencyLimit() {
        return concurrencyLimit;
    }

    @JsonIgnore
    @Override
    public Long getMemoryLimit() {
        return memoryLimit;
    }

    @JsonIgnore
    @Override
    public Long getTimeLimit() {
        return timeLimit;
    }

    @JsonIgnore
    @Override
    public Long getUserMemory() {
        return userMemory;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Activation that = (Activation) o;
        return Objects.equals(action, that.action) && Objects.equals(activationId, that.activationId) &&
                Objects.equals(revision, that.revision) && Objects.equals(kind, that.kind);
    }

    @Override
    public int hashCode() {
        return Objects.hash(action, activationId, revision, kind);
    }

    @Override
    public String toString() {
        return "Activation{" +
                "action=" + action +
                ", activationId='" + activationId + '\'' +
                ", blocking=" + blocking +
                ", cause='" + cause + '\'' +
                ", content=" + content +
                ", initArgs=" + initArgs +
                ", lockedArgs=" + lockedArgs +
                ", revision='" + revision + '\'' +
                ", rootControllerIndex=" + rootControllerIndex +
                ", transId=" + transId +
                ", user=" + user +
                ", targetInvoker='" + targetInvoker + '\'' +
                ", priority=" + priority +
                ", creationTimestamp=" + creationTimestamp +
                ", overload=" + overload +
                ", kind='" + kind + '\'' +
                ", concurrencyLimit=" + concurrencyLimit +
                ", memoryLimit=" + memoryLimit +
                ", timeLimit=" + timeLimit +
                ", userMemory=" + userMemory +
                '}';
    }

}