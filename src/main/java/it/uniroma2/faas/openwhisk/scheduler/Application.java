package it.uniroma2.faas.openwhisk.scheduler;

import it.uniroma2.faas.openwhisk.scheduler.scheduler.SchedulerComponent;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.config.Config;
import it.uniroma2.faas.openwhisk.scheduler.util.VersionProvider;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import javax.annotation.Nonnull;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.System.exit;

@CommandLine.Command(name = "ow-scheduler.jar", mixinStandardHelpOptions = true,
        versionProvider = VersionProvider.class)
public class Application implements Runnable {

    private static final Logger LOG = LogManager.getRootLogger();

    private static class Flags {
        @CommandLine.Option(names = {"-l", "--log"}, arity = "1",
                description = "Set logging levels. Supported: [ALL, TRACE, DEBUG, INFO, WARN, ERROR, FATAL, OFF].")
        private String logLevel;

        @CommandLine.Option(names = {"-b", "--kafka-bootstrap-servers"}, arity = "1",
                description = "Configure Kafka bootstrap servers.")
        private String kafkaBootstrapServer;

        @CommandLine.Option(names = {"-i", "--kafka-poll-interval"}, arity = "1",
                description = "Configure Kafka reader timeout in ms.")
        private Integer kafkaTimeoutMs;

        @CommandLine.Option(names = {"-f", "--kafka-fetch-min-bytes"}, arity = "1",
                description = "Broker wait to send records to consumer until min bytes (or max wait ms).")
        private Integer kafkaFetchMinBytes;

        @CommandLine.Option(names = {"-w", "--kafka-fetch-max-wait-ms"}, arity = "1",
                description = "Broker wait to send records to consumer until max wait ms (or min bytes).")
        private Integer kafkaFetchMaxWaitMs;

        @CommandLine.Option(names = {"-q", "--kafka-max-partition-fetch-bytes"}, arity = "1",
                description = "")
        private Integer kafkaMaxPartitionFetchBytes;

        @CommandLine.Option(names = {"-p", "--policy"}, arity = "1",
                description = "Configure scheduler policy. Supported: [PASS_THROUGH, PRIORITY_QUEUE_FIFO].")
        private String schedulerPolicy;

        @CommandLine.Option(names = {"-s", "--buffered-scheduler"},
                description = "Enable buffered scheduler functionality.")
        private boolean schedulerBuffered;

        @CommandLine.Option(names = {"-d", "--buffer-size"}, arity = "1",
                description = "Buffer size. This option takes effect with BufferedScheduler.")
        private Integer bufferSize;

        @CommandLine.Option(names = {"-B", "--invoker-buffer-limit"}, arity = "1",
                description = "Set invoker buffer limit; set to 0 to disable invoker buffering.")
        private Integer invokerBufferLimit;

        @CommandLine.Option(names = {"-k", "--heartbeat-poll"}, arity = "1",
                description = "For BufferedScheduler, set polling interval for heath kafka topic.")
        private Integer heartbeatPoll;

        @CommandLine.Option(names = {"-t", "--tracer-scheduler"},
                description = "If enabled, scheduler will trace actions belonging to composition in order to provide correct priority value.")
        private boolean schedulerTracer;

        @CommandLine.Option(names = {"-r", "--max-cmp"}, arity = "1",
                description = "Set max number of composition action when using RunningCompositionPQFIFOPolicy")
        private Integer rcpqfifoMaxCmp;
    }

    private static class Exclusive {
        @CommandLine.ArgGroup(exclusive = false)
        private Flags flags;

        @CommandLine.Option(names = {"-c", "--config-file"}, arity = "1", description = "Configuration file.")
        private String configFile;
    }

    @CommandLine.ArgGroup()
    private Exclusive exclusive;

    private void mergeOptionsWith(@Nonnull final Config config) {
        checkNotNull(config, "Config can not be null.");

        Flags flags = exclusive.flags;
        if (flags.logLevel != null)
            try {
                config.setSysLog(Level.valueOf(flags.logLevel).name());
            } catch (IllegalArgumentException e) {
                // ignore log level specified from command line
                LOG.warn("Invalid log level specified ({}). Using default level ({}).",
                        flags.logLevel, config.getSysLog());
            }
        if (flags.kafkaBootstrapServer != null)
            config.setKafkaBootstrapServers(flags.kafkaBootstrapServer);
        if (flags.kafkaTimeoutMs != null)
            config.setKafkaPollTimeoutMs(flags.kafkaTimeoutMs);
        if (flags.kafkaFetchMinBytes != null)
            config.setKafkaFetchMinBytes(flags.kafkaFetchMinBytes);
        if (flags.kafkaFetchMaxWaitMs != null)
            config.setKafkaFetchMaxWaitMs(flags.kafkaFetchMaxWaitMs);
        if (flags.kafkaMaxPartitionFetchBytes != null)
            config.setKafkaMaxPartitionFetchBytes(flags.kafkaMaxPartitionFetchBytes);
        if (flags.schedulerPolicy != null)
            config.setSchedulerPolicy(flags.schedulerPolicy);
        config.setSchedulerTracer(flags.schedulerTracer);
        config.setSchedulerBuffered(flags.schedulerBuffered);
        if (flags.bufferSize != null)
            config.setBufferedSchedulerBufferSize(flags.bufferSize);
        if (flags.heartbeatPoll != null)
            config.setSchedulerBufferedHeartbeatPoll(flags.heartbeatPoll);
        if (flags.invokerBufferLimit != null)
            config.setSchedulerBufferedInvokerBufferLimit(flags.invokerBufferLimit);
        if (flags.rcpqfifoMaxCmp != null)
            config.setPolicyRcpqfifoMaxCmp(flags.rcpqfifoMaxCmp);
    }

    private @Nonnull Config createConfig() {
        final Config config = new Config();

        try {
            if (exclusive == null) {
                config.load();
            } else if (exclusive.configFile != null) {
                // load configuration options from specified configuration file
                config.load(exclusive.configFile);
            } else {
                // load default configuration
                config.load();
                // merge configuration parameters with cli options
                // prefer cli options over configuration options
                mergeOptionsWith(config);
            }
        } catch (TypeNotPresentException | IllegalArgumentException e) {
            LOG.fatal("Error while parsing options: {}.", e.getMessage());
            exit(1);
        } catch (IOException e) {
            LOG.fatal("Error opening configuration file: {}.", e.getMessage());
            exit(1);
        } catch (Exception e) {
            LOG.fatal("Unknown error while parsing configuration file.");
            e.printStackTrace();
            exit(1);
        }

        return config;
    }

    @Override
    public void run() {
        // start scheduler
        new SchedulerComponent(createConfig()).start();
    }

    public static void main(String[] args) throws Exception {
        int exitCode = new CommandLine(new Application()).execute(args);
        System.exit(exitCode);
    }

}