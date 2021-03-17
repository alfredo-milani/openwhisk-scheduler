package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.config;

import it.uniroma2.faas.openwhisk.scheduler.util.ProjectSpecs;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.util.HashMap;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class Config extends HashMap<String, Object> {

    // Current version
    public static final String K_VERSION = "sys.version";
    // Properties filename
    public static final String K_CONFIGURATION_FILE = "sys.config";
    public static final String V_DEFAULT_CONFIGURATION_FILE = "configuration.properties";
    // Temporary directory
    public static final String K_SYS_TMP = "sys.tmp";
    public static final String V_DEFAULT_SYS_TMP = "/tmp";
    // Debug level
    public static final String K_SYS_LOG = "sys.log";
    public static final String V_DEFAULT_SYS_LOG = "INFO";

    // Kafka bootstrap server
    public static final String K_KAFKA_BOOTSTRAP_SERVERS = "scheduler.kafka.bootstrap.servers";
    public static final String V_DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    // Kafka consumer poll interval
    public static final String K_KAFKA_POLL_INTERVAL_MS = "scheduler.kafka.poll.interval";
    public static final int V_DEFAULT_KAFKA_POLL_INTERVAL_MS = 50;
    // Kafka fetch min bytes
    public static final String K_KAFKA_FETCH_MIN_BYTES = "scheduler.kafka.fetch_min_bytes";
    public static final int V_DEFAULT_KAFKA_FETCH_MIN_BYTES = 1;
    // Kafka fetch max wait ms
    public static final String K_KAFKA_FETCH_MAX_WAIT_MS = "scheduler.kafka.fetch_max_wait_ms";
    public static final int V_DEFAULT_KAFKA_FETCH_MAX_WAIT_MS = 500;
    // Kafka max partition fetch bytes
    public static final String K_KAFKA_MAX_PARTITION_FETCH_BYTES = "scheduler.kafka.max_partition_fetch_bytes";
    public static final int V_DEFAULT_KAFKA_MAX_PARTITION_FETCH_BYTES = 0;

    // Scheduler policy
    public static final String K_SCHEDULER_POLICY = "scheduler.policy";
    public static final String V_DEFAULT_SCHEDULER_POLICY = "PASS_THROUGH";
    // Scheduler buffering functionality
    public static final String K_SCHEDULER_BUFFERED = "scheduler.buffered";
    public static final boolean V_DEFAULT_SCHEDULER_BUFFERED = false;
    public static final String K_SCHEDULER_BUFFERED_BUFFER_SIZE = "scheduler.buffered.buffer_size";
    public static final int V_DEFAULT_SCHEDULER_BUFFERED_BUFFER_SIZE = 1_000;
    public static final String K_SCHEDULER_BUFFERED_INVOKER_BUFFER_LIMIT = "scheduler.buffered.invoker_buffer_limit";
    public static final int V_DEFAULT_SCHEDULER_BUFFERED_INVOKER_BUFFER_LIMIT = 0;
    public static final String K_SCHEDULER_BUFFERED_HEARTBEAT_POLL = "scheduler.buffered.heartbeat_poll";
    public static final int V_DEFAULT_SCHEDULER_BUFFERED_HEARTBEAT_POLL = 1_000;
    // Scheduler tracing functionality
    public static final String K_SCHEDULER_TRACER = "scheduler.tracer";
    public static final boolean V_DEFAULT_SCHEDULER_TRACER = false;

    public Config() {
        // intern configuration
        put(K_VERSION, ProjectSpecs.getInstance().getVersion());
        put(K_CONFIGURATION_FILE, V_DEFAULT_CONFIGURATION_FILE);

        // system section
        put(K_SYS_TMP, V_DEFAULT_SYS_TMP);
        put(K_SYS_LOG, V_DEFAULT_SYS_LOG);

        // scheduler section
        put(K_KAFKA_BOOTSTRAP_SERVERS, V_DEFAULT_KAFKA_BOOTSTRAP_SERVERS);
        put(K_KAFKA_POLL_INTERVAL_MS, V_DEFAULT_KAFKA_POLL_INTERVAL_MS);
        put(K_KAFKA_FETCH_MIN_BYTES, V_DEFAULT_KAFKA_FETCH_MIN_BYTES);
        put(K_KAFKA_FETCH_MAX_WAIT_MS, V_DEFAULT_KAFKA_FETCH_MAX_WAIT_MS);
        put(K_KAFKA_MAX_PARTITION_FETCH_BYTES, V_DEFAULT_KAFKA_MAX_PARTITION_FETCH_BYTES);

        put(K_SCHEDULER_POLICY, V_DEFAULT_SCHEDULER_POLICY);
        put(K_SCHEDULER_BUFFERED, V_DEFAULT_SCHEDULER_BUFFERED);
        put(K_SCHEDULER_BUFFERED_BUFFER_SIZE, V_DEFAULT_SCHEDULER_BUFFERED_BUFFER_SIZE);
        put(K_SCHEDULER_BUFFERED_INVOKER_BUFFER_LIMIT, V_DEFAULT_SCHEDULER_BUFFERED_INVOKER_BUFFER_LIMIT);
        put(K_SCHEDULER_BUFFERED_HEARTBEAT_POLL, V_DEFAULT_SCHEDULER_BUFFERED_HEARTBEAT_POLL);
        put(K_SCHEDULER_TRACER, V_DEFAULT_SCHEDULER_TRACER);
    }

    public void load() throws IOException {
        InputStream configInputStream = Config.class
                .getClassLoader()
                .getResourceAsStream(getConfigurationFilename());
        if (configInputStream == null) {
            throw new FileNotFoundException("Default configuration file not found");
        }

        load(configInputStream);
    }

    public void load(@Nonnull String configurationFilename) throws IOException {
        checkNotNull(
                configurationFilename,
                "ConfigurationFilename can not be null (current: %s)",
                configurationFilename
        );

        setConfigurationFilename(configurationFilename);
        load(new FileInputStream(configurationFilename));
    }

    public void load(@Nonnull InputStream configInputStream) throws IOException {
        checkNotNull(
                configInputStream,
                "ConfigInputStream can not be null (current: %s)",
                configInputStream
        );

        Properties properties = new Properties();

        try (BufferedInputStream configBufferedInputStream = new BufferedInputStream(configInputStream)) {
            properties.load(configBufferedInputStream);
        }

        // system section
        // null values will throw IllegalArgumentException
        putString(K_SYS_TMP, properties.get(K_SYS_TMP), V_DEFAULT_SYS_TMP);
        putString(K_SYS_LOG, properties.get(K_SYS_LOG), V_DEFAULT_SYS_LOG);

        // scheduler section
        putString(K_KAFKA_BOOTSTRAP_SERVERS, properties.get(K_KAFKA_BOOTSTRAP_SERVERS), V_DEFAULT_KAFKA_BOOTSTRAP_SERVERS);
        putInteger(K_KAFKA_POLL_INTERVAL_MS, properties.get(K_KAFKA_POLL_INTERVAL_MS), V_DEFAULT_KAFKA_POLL_INTERVAL_MS);
        putInteger(K_KAFKA_FETCH_MIN_BYTES, properties.get(K_KAFKA_FETCH_MIN_BYTES), V_DEFAULT_KAFKA_FETCH_MIN_BYTES);
        putInteger(K_KAFKA_FETCH_MAX_WAIT_MS, properties.get(K_KAFKA_FETCH_MAX_WAIT_MS), V_DEFAULT_KAFKA_FETCH_MAX_WAIT_MS);
        putInteger(K_KAFKA_MAX_PARTITION_FETCH_BYTES, properties.get(K_KAFKA_MAX_PARTITION_FETCH_BYTES), V_DEFAULT_KAFKA_MAX_PARTITION_FETCH_BYTES);

        putString(K_SCHEDULER_POLICY, properties.get(K_SCHEDULER_POLICY), null);
        putBoolean(K_SCHEDULER_BUFFERED, properties.get(K_SCHEDULER_BUFFERED), V_DEFAULT_SCHEDULER_BUFFERED);
        putInteger(K_SCHEDULER_BUFFERED_BUFFER_SIZE, properties.get(K_SCHEDULER_BUFFERED_BUFFER_SIZE), V_DEFAULT_SCHEDULER_BUFFERED_BUFFER_SIZE);
        putInteger(K_SCHEDULER_BUFFERED_INVOKER_BUFFER_LIMIT, properties.get(K_SCHEDULER_BUFFERED_INVOKER_BUFFER_LIMIT), V_DEFAULT_SCHEDULER_BUFFERED_INVOKER_BUFFER_LIMIT);
        putInteger(K_SCHEDULER_BUFFERED_HEARTBEAT_POLL, properties.get(K_SCHEDULER_BUFFERED_HEARTBEAT_POLL), V_DEFAULT_SCHEDULER_BUFFERED_HEARTBEAT_POLL);
        putBoolean(K_SCHEDULER_TRACER, properties.get(K_SCHEDULER_TRACER), V_DEFAULT_SCHEDULER_TRACER);
    }

    private void putObject(@Nonnull String key, @Nullable Object value, @Nullable Object defaultValue) {
        checkNotNull(key, "Key can not be null.");
        if (value != null) {
            put(key, value);
        } else if (defaultValue != null) {
            put(key, defaultValue);
        } else {
            throw new IllegalArgumentException(String.format("Missing value for key '%s'", key));
        }
    }

    private void putString(@Nonnull String key, @Nullable Object value, @Nullable Object defaultValue) {
        checkNotNull(key, "Key can not be null.");
        if (value != null) {
            put(key, String.valueOf(value));
        } else if (defaultValue != null) {
            put(key, defaultValue);
        } else {
            throw new IllegalArgumentException(String.format("Missing value for key '%s'", key));
        }
    }

    private void putInteger(@Nonnull String key, @Nullable Object value, @Nullable Object defaultValue) {
        checkNotNull(key, "Key can not be null.");
        if (value != null) {
            // could be inserted a try-catch block to ensure value as Integer
            put(key, Integer.valueOf((String) value));
        } else if (defaultValue != null) {
            put(key, defaultValue);
        } else {
            throw new IllegalArgumentException(String.format("Missing value for key '%s'", key));
        }
    }

    private void putLong(@Nonnull String key, @Nullable Object value, @Nullable Object defaultValue) {
        checkNotNull(key, "Key can not be null.");
        if (value != null) {
            put(key, Long.valueOf((String) value));
        } else if (defaultValue != null) {
            put(key, defaultValue);
        } else {
            throw new IllegalArgumentException(String.format("Missing value for key '%s'", key));
        }
    }

    private void putFloat(@Nonnull String key, @Nullable Object value, @Nullable Object defaultValue) {
        checkNotNull(key, "Key can not be null.");
        if (value != null) {
            put(key, Float.valueOf((String) value));
        } else if (defaultValue != null) {
            put(key, defaultValue);
        } else {
            throw new IllegalArgumentException(String.format("Missing value for key '%s'", key));
        }
    }

    private void putDouble(@Nonnull String key, @Nullable Object value, @Nullable Object defaultValue) {
        checkNotNull(key, "Key can not be null.");
        if (value != null) {
            put(key, Double.valueOf((String) value));
        } else if (defaultValue != null) {
            put(key, defaultValue);
        } else {
            throw new IllegalArgumentException(String.format("Missing value for key '%s'", key));
        }
    }

    private void putBoolean(@Nonnull String key, @Nullable Object value, @Nullable Object defaultValue) {
        checkNotNull(key, "Key can not be null (current: %s)", key);
        if (value != null) {
            put(key, Boolean.valueOf((String) value));
        } else if (defaultValue != null) {
            put(key, defaultValue);
        } else {
            throw new IllegalArgumentException(String.format("Missing value for key '%s'", key));
        }
    }

    public String getVersion() {
        return (String) get(K_VERSION);
    }

    public String getConfigurationFilename() {
        return (String) get(K_CONFIGURATION_FILE);
    }

    public void setConfigurationFilename(@Nonnull String configurationFile) {
        checkNotNull(
                configurationFile,
                "ConfigurationFile can not be null (current: %s)",
                configurationFile
        );

        put(K_CONFIGURATION_FILE, configurationFile);
    }

    public String getSysTmp() {
        return (String) get(K_SYS_TMP);
    }

    public void setSysTmp(@Nonnull String sysTmp) {
        checkNotNull(sysTmp, "System temporary directory can not be null.");
        put(K_SYS_TMP, sysTmp);
    }

    public String getSysLog() {
        return (String) get(K_SYS_LOG);
    }

    public void setSysLog(String sysDebug) {
        put(K_SYS_LOG, sysDebug);
    }

    public String getKafkaBootstrapServers() {
        return (String) get(K_KAFKA_BOOTSTRAP_SERVERS);
    }

    public void setKafkaBootstrapServers(@Nonnull String kafkaBootstrapServers) {
        checkNotNull(kafkaBootstrapServers, "Kafka bootstrap server can not be null.");
        put(K_KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers);
    }

    public int getKafkaPollTimeoutMs() {
        return (int) get(K_KAFKA_POLL_INTERVAL_MS);
    }

    public void setKafkaPollTimeoutMs(int pollTimeoutMs) {
        put(K_KAFKA_POLL_INTERVAL_MS, pollTimeoutMs);
    }

    public int getKafkaFetchMinBytes() {
        return (int) get(K_KAFKA_FETCH_MIN_BYTES);
    }

    public void setKafkaFetchMinBytes(int kafkaFetchMinBytes) {
        put(K_KAFKA_FETCH_MIN_BYTES, kafkaFetchMinBytes);
    }

    public int getKafkaFetchMaxWaitMs() {
        return (int) get(K_KAFKA_FETCH_MAX_WAIT_MS);
    }

    public void setKafkaFetchMaxWaitMs(int kafkaFetchMaxWaitMs) {
        put(K_KAFKA_FETCH_MAX_WAIT_MS, kafkaFetchMaxWaitMs);
    }

    public int getKafkaMaxPartitionFetchBytes() {
        return (int) get(K_KAFKA_MAX_PARTITION_FETCH_BYTES);
    }

    public void setKafkaMaxPartitionFetchBytes(int kafkaMaxPartitionFetchBytes) {
        put(K_KAFKA_MAX_PARTITION_FETCH_BYTES, kafkaMaxPartitionFetchBytes);
    }

    public String getSchedulerPolicy() {
        return (String) get(K_SCHEDULER_POLICY);
    }

    public void setSchedulerPolicy(@Nonnull String schedulerPolicy) {
        checkNotNull(schedulerPolicy, "Scheduler policy can not be null.");
        put(K_SCHEDULER_POLICY, schedulerPolicy);
    }

    public boolean getSchedulerBuffered() {
        return (boolean) get(K_SCHEDULER_BUFFERED);
    }

    public void setSchedulerBuffered(boolean schedulerBuffered) {
        put(K_SCHEDULER_BUFFERED, schedulerBuffered);
    }

    public int getSchedulerBufferedBufferSize() {
        return (int) get(K_SCHEDULER_BUFFERED_BUFFER_SIZE);
    }

    public void setBufferedSchedulerBufferSize(int bufferSize) {
        checkArgument(bufferSize > 0, "Buffer size must be > 0.");
        put(K_SCHEDULER_BUFFERED_BUFFER_SIZE, bufferSize);
    }

    public int getSchedulerBufferedInvokerBufferLimit() {
        return (int) get(K_SCHEDULER_BUFFERED_INVOKER_BUFFER_LIMIT);
    }

    public void setSchedulerBufferedInvokerBufferLimit(int invokerBufferLimit) {
        checkArgument(invokerBufferLimit >= 0, "Invoker buffer limit must be >= 0.");
        put(K_SCHEDULER_BUFFERED_INVOKER_BUFFER_LIMIT, invokerBufferLimit);
    }

    public int getSchedulerBufferedHeartbeatPoll() {
        return (int) get(K_SCHEDULER_BUFFERED_HEARTBEAT_POLL);
    }

    public void setSchedulerBufferedHeartbeatPoll(int polling) {
        checkArgument(polling >= 0, "Polling interval must be >= 0.");
        put(K_SCHEDULER_BUFFERED_HEARTBEAT_POLL, polling);
    }

    public boolean getSchedulerTracer() {
        return (boolean) get(K_SCHEDULER_TRACER);
    }

    public void setSchedulerTracer(boolean schedulerTracer) {
        put(K_SCHEDULER_TRACER, schedulerTracer);
    }

    @Override
    public String toString() {
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(String.format("Properties for %s:", this.getClass().getSimpleName()));
        keySet().forEach(k -> stringBuilder.append(String.format("\n\t> %s = %s", k, get(k))));
        return stringBuilder.toString();
    }

}