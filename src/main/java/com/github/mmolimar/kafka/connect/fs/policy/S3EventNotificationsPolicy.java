package com.github.mmolimar.kafka.connect.fs.policy;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.FileMetadata;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

public class S3EventNotificationsPolicy extends AbstractPolicy {

    private static final Logger log = LoggerFactory.getLogger(S3EventNotificationsPolicy.class);
    private static final long DEFAULT_POLL = 5000L;
    private static final String S3_EVENT_NOTIFICATIONS_POLICY_PREFIX = FsSourceTaskConfig.POLICY_PREFIX +
            "s3_event_notifications.";

    public static final String S3_EVENT_NOTIFICATIONS_POLICY_QUEUE = S3_EVENT_NOTIFICATIONS_POLICY_PREFIX + "queue";
    public static final String S3_EVENT_NOTIFICATIONS_POLICY_POLL_MS = S3_EVENT_NOTIFICATIONS_POLICY_PREFIX + "poll";
    public static final String S3_EVENT_NOTIFICATIONS_POLICY_EVENT_REGEX = S3_EVENT_NOTIFICATIONS_POLICY_PREFIX + "event_regex";
    public static final String S3_EVENT_NOTIFICATIONS_POLICY_DELETE_MESSAGES = S3_EVENT_NOTIFICATIONS_POLICY_PREFIX + "delete_messages";
    public static final String S3_EVENT_NOTIFICATIONS_POLICY_MAX_MESSAGES = S3_EVENT_NOTIFICATIONS_POLICY_PREFIX + "max_messages";
    public static final String S3_EVENT_NOTIFICATIONS_POLICY_VISIBILITY_TIMEOUT = S3_EVENT_NOTIFICATIONS_POLICY_PREFIX + "visibility_timeout";

    private final Time time;
    private final ObjectMapper mapper;
    private final AmazonSQS sqs;
    private final ReceiveMessageRequest request;
    private final String queueUrl;

    private String queue;
    private String eventNameRegex;
    private Integer maxNumberOfMessages;
    private Integer visibilityTimeout;
    private long pollSleepMs;
    private boolean isShutDown;
    private boolean deleteMessages;

    public S3EventNotificationsPolicy(FsSourceTaskConfig conf) throws IOException {
        super(conf);

        this.time = new SystemTime();
        this.mapper = new ObjectMapper();
        this.sqs = getSqsClient();
        this.queueUrl = this.sqs.getQueueUrl(getQueue()).getQueueUrl();
        this.request = new ReceiveMessageRequest()
                .withMaxNumberOfMessages(maxNumberOfMessages)
                .withVisibilityTimeout(visibilityTimeout)
                .withQueueUrl(queueUrl);
        this.isShutDown = false;
    }

    @Override
    protected void configPolicy(Map<String, Object> customConfigs) {
        try {
            this.pollSleepMs = Long.parseLong((String) customConfigs
                    .getOrDefault(S3_EVENT_NOTIFICATIONS_POLICY_POLL_MS, String.valueOf(DEFAULT_POLL)));
        } catch (NumberFormatException nfe) {
            throw new ConfigException(S3_EVENT_NOTIFICATIONS_POLICY_POLL_MS + " property is required and must be a " +
                    "number (long). Got: " + customConfigs.get(S3_EVENT_NOTIFICATIONS_POLICY_POLL_MS));
        }

        this.eventNameRegex = customConfigs.getOrDefault(S3_EVENT_NOTIFICATIONS_POLICY_EVENT_REGEX, ".*").toString();

        try {
            if (customConfigs.containsKey(S3_EVENT_NOTIFICATIONS_POLICY_MAX_MESSAGES)) {
                this.maxNumberOfMessages = Integer.valueOf((String) customConfigs
                        .get(S3_EVENT_NOTIFICATIONS_POLICY_MAX_MESSAGES));
                if (this.maxNumberOfMessages < 1 || this.maxNumberOfMessages > 10) {
                    throw new IllegalArgumentException("Max number of messages must be between 1 and 10.");
                }
            }
        } catch (IllegalArgumentException e) {
            throw new ConfigException(S3_EVENT_NOTIFICATIONS_POLICY_MAX_MESSAGES + " property is required and " +
                    "must be an integer between 1 and 10. Got: " + customConfigs.get(S3_EVENT_NOTIFICATIONS_POLICY_MAX_MESSAGES));
        }

        try {
            if (customConfigs.containsKey(S3_EVENT_NOTIFICATIONS_POLICY_VISIBILITY_TIMEOUT)) {
                this.visibilityTimeout = Integer.valueOf((String) customConfigs
                        .get(S3_EVENT_NOTIFICATIONS_POLICY_VISIBILITY_TIMEOUT));
            }
        } catch (NumberFormatException nfe) {
            throw new ConfigException(S3_EVENT_NOTIFICATIONS_POLICY_VISIBILITY_TIMEOUT + " property is required and " +
                    "must be a number (int). Got: " + customConfigs.get(S3_EVENT_NOTIFICATIONS_POLICY_VISIBILITY_TIMEOUT));
        }

        if (customConfigs.get(S3_EVENT_NOTIFICATIONS_POLICY_QUEUE) == null ||
                customConfigs.get(S3_EVENT_NOTIFICATIONS_POLICY_QUEUE).toString().trim().equals("")) {
            throw new ConfigException(S3_EVENT_NOTIFICATIONS_POLICY_QUEUE + " cannot be empty. Got: '" +
                    customConfigs.get(S3_EVENT_NOTIFICATIONS_POLICY_QUEUE) + "'.");
        }
        this.queue = customConfigs.get(S3_EVENT_NOTIFICATIONS_POLICY_QUEUE).toString().trim();
        this.deleteMessages = Boolean.parseBoolean(customConfigs
                .getOrDefault(S3_EVENT_NOTIFICATIONS_POLICY_DELETE_MESSAGES, "true").toString());
    }

    @Override
    public Iterator<FileMetadata> listFiles(FileSystem fs) {
        return sqs.receiveMessage(request).getMessages()
                .stream()
                .flatMap(message -> parseMessage(message).stream())
                .filter(record -> record.eventName.matches(eventNameRegex))
                .filter(record -> fs.getWorkingDirectory().toString().startsWith(getUriPrefix() + record.bucketName))
                .map(record -> {
                    Path path = new Path(getUriPrefix() + record.bucketName + "/", record.objectKey);
                    Optional<FileMetadata> metadata = Optional.empty();
                    try {
                        RemoteIterator<LocatedFileStatus> it = fs.listFiles(path, false);
                        if (it.hasNext()) {
                            LocatedFileStatus status = it.next();
                            if (status.isFile()) metadata = Optional.of(toMetadata(status));
                        }
                    } catch (Exception ioe) {
                        log.warn("{} Cannot get file at path '{}': {}", this, path, ioe.getMessage());
                    }
                    if (deleteMessages) {
                        log.trace("{} Removing message with ID '{}'.", this, record.messageId);
                        sqs.deleteMessage(queueUrl, record.receiptHandle);
                    }

                    return metadata;
                })
                .flatMap(metadataOpt -> metadataOpt.map(Stream::of).orElseGet(Stream::empty))
                .iterator();
    }

    private List<EventRecord> parseMessage(Message message) {
        List<EventRecord> events = new ArrayList<>();
        try {
            JsonNode content = mapper.readTree(message.getBody());
            if (content.has("Type") && content.get("Type").asText().equals("Notification")) {
                content = mapper.readTree(content.get("Message").asText());
            }
            if (!content.has("Records")) {
                return events;
            }
            for (JsonNode record : content.get("Records")) {
                String eventName = record.get("eventName").asText();
                if (record.has("s3")) {
                    String bucketName = record.get("s3").get("bucket").get("name").asText();
                    String objectKey = record.get("s3").get("object").get("key").asText();
                    events.add(new EventRecord(message.getMessageId(), message.getReceiptHandle(),
                            eventName, bucketName, objectKey));
                }
            }
        } catch (JsonProcessingException jpe) {
            log.debug("{} Ignoring event due to cannot be parsed. Value: '{}'.", this, message.getBody());
        }
        return events;
    }

    private static class EventRecord {
        final String messageId;
        final String receiptHandle;
        final String eventName;
        final String bucketName;
        final String objectKey;

        public EventRecord(String messageId, String receiptHandle, String eventName, String bucketName, String objectKey) {
            this.messageId = messageId;
            this.receiptHandle = receiptHandle;
            this.eventName = eventName;
            this.bucketName = bucketName;
            // cleaning spaces
            this.objectKey = objectKey.replace("+", " ");
        }
    }

    protected String getUriPrefix() {
        return "s3a://";
    }

    protected AmazonSQS getSqsClient() {
        return AmazonSQSClientBuilder.defaultClient();
    }

    protected String getQueue() {
        return queue;
    }

    @Override
    protected boolean isPolicyCompleted() {
        return isShutDown;
    }

    @Override
    public void interrupt() {
        shutdown();
        super.interrupt();
    }

    @Override
    public void postCheck() {
        time.sleep(pollSleepMs);
    }

    @Override
    public void close() throws IOException {
        shutdown();
        super.close();
    }

    private void shutdown() {
        isShutDown = true;
        sqs.shutdown();
    }

}
