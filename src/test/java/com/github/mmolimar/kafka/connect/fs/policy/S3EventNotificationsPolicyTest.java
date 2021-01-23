package com.github.mmolimar.kafka.connect.fs.policy;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.FileMetadata;
import com.github.mmolimar.kafka.connect.fs.file.reader.TextFileReader;
import com.github.mmolimar.kafka.connect.fs.util.ReflectionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.powermock.api.easymock.PowerMock;

import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class S3EventNotificationsPolicyTest extends PolicyTestBase {

    static {
        TEST_FILE_SYSTEMS = Collections.singletonList(
                new LocalFsConfig()
        );
    }

    @BeforeAll
    public static void initFs() throws IOException {
        for (PolicyFsTestConfig fsConfig : TEST_FILE_SYSTEMS) {
            fsConfig.initFs();
        }
    }

    @Override
    protected FsSourceTaskConfig buildSourceTaskConfig(List<Path> directories) {
        Map<String, String> cfg = new HashMap<String, String>() {{
            String[] uris = directories.stream().map(Path::toString)
                    .toArray(String[]::new);
            put(FsSourceTaskConfig.FS_URIS, String.join(",", uris));
            put(FsSourceTaskConfig.TOPIC, "topic_test");
            put(FsSourceTaskConfig.POLICY_CLASS, S3EventNotificationsPolicyMock.class.getName());
            put(FsSourceTaskConfig.FILE_READER_CLASS, TextFileReader.class.getName());
            put(FsSourceTaskConfig.POLICY_REGEXP, "^[0-9]*\\.txt$");
            put(FsSourceTaskConfig.POLICY_PREFIX_FS + "dfs.data.dir", "test");
            put(FsSourceTaskConfig.POLICY_PREFIX_FS + "fs.default.name", "hdfs://test");
            put(S3EventNotificationsPolicy.S3_EVENT_NOTIFICATIONS_POLICY_QUEUE, "test");
        }};
        return new FsSourceTaskConfig(cfg);
    }

    // This policy does not throw any exception. Just stop watching those nonexistent dirs
    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    @Override
    public void invalidDirectory(PolicyFsTestConfig fsConfig) throws IOException {
        for (Path dir : fsConfig.getDirectories()) {
            fsConfig.getFs().delete(dir, true);
        }
        try {
            fsConfig.getPolicy().execute();
        } finally {
            for (Path dir : fsConfig.getDirectories()) {
                fsConfig.getFs().mkdirs(dir);
            }
        }
    }

    // This policy never ends. We have to interrupt it
    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    @Override
    public void execPolicyAlreadyEnded(PolicyFsTestConfig fsConfig) throws IOException {
        fsConfig.getPolicy().execute();
        assertFalse(fsConfig.getPolicy().hasEnded());
        fsConfig.getPolicy().interrupt();
        assertTrue(fsConfig.getPolicy().hasEnded());
        assertThrows(IllegalWorkerStateException.class, () -> fsConfig.getPolicy().execute());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    @SuppressWarnings("unchecked")
    public void invalidPollTime(PolicyFsTestConfig fsConfig) {
        Map<String, String> originals = fsConfig.getSourceTaskConfig().originalsStrings();
        originals.put(S3EventNotificationsPolicy.S3_EVENT_NOTIFICATIONS_POLICY_POLL_MS, "invalid");
        FsSourceTaskConfig cfg = new FsSourceTaskConfig(originals);
        assertThrows(ConnectException.class, () ->
                ReflectionUtils.makePolicy((Class<? extends Policy>) fsConfig.getSourceTaskConfig()
                        .getClass(FsSourceTaskConfig.POLICY_CLASS), cfg));
        assertThrows(ConfigException.class, () -> {
            try {
                ReflectionUtils.makePolicy((Class<? extends Policy>) fsConfig.getSourceTaskConfig()
                        .getClass(FsSourceTaskConfig.POLICY_CLASS), cfg);
            } catch (Exception e) {
                throw e.getCause();
            }
        });
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    @SuppressWarnings("unchecked")
    public void invalidMaxMessages(PolicyFsTestConfig fsConfig) {
        Map<String, String> originals = fsConfig.getSourceTaskConfig().originalsStrings();
        originals.put(S3EventNotificationsPolicy.S3_EVENT_NOTIFICATIONS_POLICY_MAX_MESSAGES, "invalid");
        FsSourceTaskConfig cfgInvalid = new FsSourceTaskConfig(originals);
        assertThrows(ConnectException.class, () ->
                ReflectionUtils.makePolicy((Class<? extends Policy>) fsConfig.getSourceTaskConfig()
                        .getClass(FsSourceTaskConfig.POLICY_CLASS), cfgInvalid));
        assertThrows(ConfigException.class, () -> {
            try {
                ReflectionUtils.makePolicy((Class<? extends Policy>) fsConfig.getSourceTaskConfig()
                        .getClass(FsSourceTaskConfig.POLICY_CLASS), cfgInvalid);
            } catch (Exception e) {
                throw e.getCause();
            }
        });

        originals.put(S3EventNotificationsPolicy.S3_EVENT_NOTIFICATIONS_POLICY_MAX_MESSAGES, "100");
        FsSourceTaskConfig cfgMaxMessages = new FsSourceTaskConfig(originals);
        assertThrows(ConnectException.class, () ->
                ReflectionUtils.makePolicy((Class<? extends Policy>) fsConfig.getSourceTaskConfig()
                        .getClass(FsSourceTaskConfig.POLICY_CLASS), cfgMaxMessages));
        assertThrows(ConfigException.class, () -> {
            try {
                ReflectionUtils.makePolicy((Class<? extends Policy>) fsConfig.getSourceTaskConfig()
                        .getClass(FsSourceTaskConfig.POLICY_CLASS), cfgMaxMessages);
            } catch (Exception e) {
                throw e.getCause();
            }
        });
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    @SuppressWarnings("unchecked")
    public void customMaxMessages(PolicyFsTestConfig fsConfig) {
        Map<String, String> originals = fsConfig.getSourceTaskConfig().originalsStrings();
        originals.put(S3EventNotificationsPolicy.S3_EVENT_NOTIFICATIONS_POLICY_MAX_MESSAGES, "5");
        FsSourceTaskConfig cfg = new FsSourceTaskConfig(originals);
        assertDoesNotThrow(() ->
                ReflectionUtils.makePolicy((Class<? extends Policy>) fsConfig.getSourceTaskConfig()
                        .getClass(FsSourceTaskConfig.POLICY_CLASS), cfg));
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    @SuppressWarnings("unchecked")
    public void invalidVisibilityTimeout(PolicyFsTestConfig fsConfig) {
        Map<String, String> originals = fsConfig.getSourceTaskConfig().originalsStrings();
        originals.put(S3EventNotificationsPolicy.S3_EVENT_NOTIFICATIONS_POLICY_VISIBILITY_TIMEOUT, "invalid");
        FsSourceTaskConfig cfg = new FsSourceTaskConfig(originals);
        assertThrows(ConnectException.class, () ->
                ReflectionUtils.makePolicy((Class<? extends Policy>) fsConfig.getSourceTaskConfig()
                        .getClass(FsSourceTaskConfig.POLICY_CLASS), cfg));
        assertThrows(ConfigException.class, () -> {
            try {
                ReflectionUtils.makePolicy((Class<? extends Policy>) fsConfig.getSourceTaskConfig()
                        .getClass(FsSourceTaskConfig.POLICY_CLASS), cfg);
            } catch (Exception e) {
                throw e.getCause();
            }
        });
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    @SuppressWarnings("unchecked")
    public void customVisibilityTimeout(PolicyFsTestConfig fsConfig) {
        Map<String, String> originals = fsConfig.getSourceTaskConfig().originalsStrings();
        originals.put(S3EventNotificationsPolicy.S3_EVENT_NOTIFICATIONS_POLICY_VISIBILITY_TIMEOUT, "1");
        FsSourceTaskConfig cfg = new FsSourceTaskConfig(originals);
        assertDoesNotThrow(() ->
                ReflectionUtils.makePolicy((Class<? extends Policy>) fsConfig.getSourceTaskConfig()
                        .getClass(FsSourceTaskConfig.POLICY_CLASS), cfg));
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    @SuppressWarnings("unchecked")
    public void withoutQueue(PolicyFsTestConfig fsConfig) {
        Map<String, String> originals = fsConfig.getSourceTaskConfig().originalsStrings();
        originals.remove(S3EventNotificationsPolicy.S3_EVENT_NOTIFICATIONS_POLICY_QUEUE);
        FsSourceTaskConfig cfg = new FsSourceTaskConfig(originals);
        assertThrows(ConnectException.class, () ->
                ReflectionUtils.makePolicy((Class<? extends Policy>) fsConfig.getSourceTaskConfig()
                        .getClass(FsSourceTaskConfig.POLICY_CLASS), cfg));
        assertThrows(ConfigException.class, () -> {
            try {
                ReflectionUtils.makePolicy((Class<? extends Policy>) fsConfig.getSourceTaskConfig()
                        .getClass(FsSourceTaskConfig.POLICY_CLASS), cfg);
            } catch (Exception e) {
                throw e.getCause();
            }
        });
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    @Disabled
    public void execPolicyBatchesFiles(PolicyFsTestConfig fsConfig) {
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    @Disabled
    public void recursiveDirectory(PolicyFsTestConfig fsConfig) {
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    @Override
    public void oneFilePerFs(PolicyFsTestConfig fsConfig) throws IOException {
        FileSystem fs = fsConfig.getFs();
        Path testDir = new Path(System.getProperty("java.io.tmpdir"));
        for (Path dir : fsConfig.getDirectories()) {
            testDir = new Path(dir.getParent().getParent(), "test");
            fs.mkdirs(testDir);
            fs.createNewFile(new Path(testDir, "0123456789.txt"));
        }
        Iterator<FileMetadata> it = fsConfig.getPolicy().execute();
        assertTrue(it.hasNext());
        it.next();
        assertTrue(it.hasNext());
        it.next();
        assertFalse(it.hasNext());
        fs.delete(testDir, true);
    }

    public static class S3EventNotificationsPolicyMock extends S3EventNotificationsPolicy {

        public S3EventNotificationsPolicyMock(FsSourceTaskConfig conf) throws IOException {
            super(conf);
        }

        @Override
        protected String getUriPrefix() {
            return "file:" + System.getProperty("java.io.tmpdir");
        }

        @Override
        protected AmazonSQS getSqsClient() {
            AmazonSQS sqs = PowerMock.createMock(AmazonSQS.class);

            EasyMock.expect(sqs.getQueueUrl(getQueue()))
                    .andReturn(new GetQueueUrlResult().withQueueUrl(getQueue()))
                    .anyTimes();

            ReceiveMessageResult messageResult = new ReceiveMessageResult();
            List<Message> messages = new ArrayList<>();
            messages.add(new Message().withBody(MESSAGE_RAW));
            messages.add(new Message().withBody(MESSAGE_SNS));
            messages.add(new Message().withBody(MESSAGE_NO_RECORDS));
            messages.add(new Message().withBody(MESSAGE_INVALID));
            messageResult.setMessages(messages);
            Capture<ReceiveMessageRequest> receiveRequest = Capture.newInstance(CaptureType.ALL);
            EasyMock.expect(sqs.receiveMessage(EasyMock.capture(receiveRequest)))
                    .andReturn(messageResult)
                    .anyTimes();

            Capture<String> anyString = Capture.newInstance(CaptureType.ALL);
            EasyMock.expect(sqs.deleteMessage(EasyMock.capture(anyString), EasyMock.capture(anyString)))
                    .andAnswer(() -> null)
                    .anyTimes();

            sqs.shutdown();
            EasyMock.expectLastCall().andVoid().anyTimes();

            EasyMock.checkOrder(sqs, false);
            EasyMock.replay(sqs);

            return sqs;
        }

        private static final String MESSAGE_RAW = "{\n" +
                "  \"Records\" : [ {\n" +
                "    \"eventVersion\" : \"2.1\",\n" +
                "    \"eventSource\" : \"aws:s3\",\n" +
                "    \"awsRegion\" : \"us-west-1\",\n" +
                "    \"eventTime\" : \"2021-01-01T01:01:01.001Z\",\n" +
                "    \"eventName\" : \"ObjectCreated:Put\",\n" +
                "    \"userIdentity\" : {\n" +
                "      \"principalId\" : \"AWS:AROA5EJEASFBABPAO11B11:test\"\n" +
                "    },\n" +
                "    \"requestParameters\" : {\n" +
                "      \"sourceIPAddress\" : \"127.0.0.1\"\n" +
                "    },\n" +
                "    \"responseElements\" : {\n" +
                "      \"x-amz-request-id\" : \"ACA811A1BD123C0F\",\n" +
                "      \"x-amz-id-2\" : \"2r0F/Yw4hv6Sweqq91fEZlGam0tr4ScKMRWZA1LjOlC1RW/h4Xz45asxwoDpHDAK9f1ba\"\n" +
                "    },\n" +
                "    \"s3\" : {\n" +
                "      \"s3SchemaVersion\" : \"1.0\",\n" +
                "      \"configurationId\" : \"test\",\n" +
                "      \"bucket\" : {\n" +
                "        \"name\" : \"test\",\n" +
                "        \"ownerIdentity\" : {\n" +
                "          \"principalId\" : \"ACA811A1BD123C0F\"\n" +
                "        },\n" +
                "        \"arn\" : \"arn:aws:s3:::test\"\n" +
                "      },\n" +
                "      \"object\" : {\n" +
                "        \"key\" : \"0123456789.txt\",\n" +
                "        \"size\" : 0,\n" +
                "        \"eTag\" : \"d41d8cd98f13g204e9239115xf8427e\",\n" +
                "        \"sequencer\" : \"00211B24AA43A01199\"\n" +
                "      }\n" +
                "    }\n" +
                "  } ]\n" +
                "}\n";

        private static final String MESSAGE_SNS = "{\n" +
                "  \"Type\": \"Notification\",\n" +
                "  \"MessageId\": \"a3091de2-0f4d-51e8-a6fb-456737913670\",\n" +
                "  \"TopicArn\": \"arn:aws:sns:us-west-1:953167085750:test\",\n" +
                "  \"Subject\": \"Amazon S3 Notification\",\n" +
                "  \"Message\": \"" + "{\\\"Records\\\":[{\\\"eventVersion\\\":\\\"2.1\\\",\\\"eventSource\\\":\\\"aws:s3\\\",\\\"awsRegion\\\":\\\"us-west-1\\\",\\\"eventTime\\\":\\\"2021-01-01T01:01:01.001Z\\\",\\\"eventName\\\":\\\"ObjectCreated:Put\\\",\\\"userIdentity\\\":{\\\"principalId\\\":\\\"AWS:AROA5EJEASFBABPAO7BB11:test\\\"},\\\"requestParameters\\\":{\\\"sourceIPAddress\\\":\\\"127.0.0.1\\\"},\\\"responseElements\\\":{\\\"x-amz-request-id\\\":\\\"ACA8110A0D438C0F\\\",\\\"x-amz-id-2\\\":\\\"2r0F\\/Yi6hv6Sweqq91fEZlGam0tr4ScKMRWZA1LjOlC1RW\\/h4XzYjxHwoDpHDAK9f1ba\\\"},\\\"s3\\\":{\\\"s3SchemaVersion\\\":\\\"1.0\\\",\\\"configurationId\\\":\\\"test\\\",\\\"bucket\\\":{\\\"name\\\":\\\"test\\\",\\\"ownerIdentity\\\":{\\\"principalId\\\":\\\"ACA8110A0D438C0F\\\"},\\\"arn\\\":\\\"arn:aws:s3:::test\\\"},\\\"object\\\":{\\\"key\\\":\\\"f1\\/file.txt\\\",\\\"size\\\":0,\\\"eTag\\\":\\\"d41d8cd98f11b204e9239124cf8427e\\\",\\\"sequencer\\\":\\\"00200C24AA43A01199\\\"}}}]}" + "\",\n" +
                "  \"Timestamp\": \"2020-01-01T01:01:01.0001Z\",\n" +
                "  \"SignatureVersion\": \"1\",\n" +
                "  \"Signature\": \"pXQWpasLFJ3ecRAX0rcqwevxZpBSErUgcTqqIS4VAJtdQ7y5fqjEg+veJHIpBIBtd2bEUN7JMNkm8\",\n" +
                "  \"SigningCertURL\": \"https://sns.us-west-1.amazonaws.com/SimpleNotificationService-010a517c1823535cd94bdb92c3asdf.pem\",\n" +
                "  \"UnsubscribeURL\": \"https://sns.us-west-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-west-1:953167085750:test:8419c54c-316d-1234-bc6c-acbcede97e47\"\n" +
                "}";

        private static final String MESSAGE_NO_RECORDS = "{}";

        private static final String MESSAGE_INVALID = "invalid";

    }


}
