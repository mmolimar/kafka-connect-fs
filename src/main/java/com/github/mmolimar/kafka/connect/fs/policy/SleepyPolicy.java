package com.github.mmolimar.kafka.connect.fs.policy;

import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class SleepyPolicy extends AbstractPolicy {

    private static final Logger log = LoggerFactory.getLogger(SleepyPolicy.class);

    private static final int DEFAULT_SLEEP_FRACTION = 10;
    private static final int DEFAULT_MAX_EXECS = -1;
    private static final String SLEEPY_POLICY_PREFIX = FsSourceTaskConfig.POLICY_PREFIX + "sleepy.";

    public static final String SLEEPY_POLICY_SLEEP_MS = SLEEPY_POLICY_PREFIX + "sleep";
    public static final String SLEEPY_POLICY_SLEEP_FRACTION = SLEEPY_POLICY_PREFIX + "fraction";
    public static final String SLEEPY_POLICY_MAX_EXECS = SLEEPY_POLICY_PREFIX + "max_execs";

    private long sleep;
    private long sleepFraction;
    private long maxExecs;

    public SleepyPolicy(FsSourceTaskConfig conf) throws IOException {
        super(conf);
    }

    @Override
    protected void configPolicy(Map<String, Object> customConfigs) {
        try {
            this.sleep = Long.parseLong((String) customConfigs.get(SLEEPY_POLICY_SLEEP_MS));
        } catch (NumberFormatException nfe) {
            throw new ConfigException(SLEEPY_POLICY_SLEEP_MS + " property is required and must be a number (long). Got: " +
                    customConfigs.get(SLEEPY_POLICY_SLEEP_MS));
        }
        try {
            this.maxExecs = Long.parseLong((String) customConfigs.getOrDefault(SLEEPY_POLICY_MAX_EXECS,
                    String.valueOf(DEFAULT_MAX_EXECS)));
        } catch (NumberFormatException nfe) {
            throw new ConfigException(SLEEPY_POLICY_MAX_EXECS + " property must be a number (long). Got: " +
                    customConfigs.get(SLEEPY_POLICY_MAX_EXECS));
        }
        try {
            this.sleepFraction = Long.parseLong((String) customConfigs.getOrDefault(SLEEPY_POLICY_SLEEP_FRACTION,
                    String.valueOf(DEFAULT_SLEEP_FRACTION)));
        } catch (NumberFormatException nfe) {
            throw new ConfigException(SLEEPY_POLICY_SLEEP_FRACTION + " property must be a number (long). Got: " +
                    customConfigs.get(SLEEPY_POLICY_SLEEP_FRACTION));
        }
    }

    @Override
    protected void preCheck() {
        sleepIfApply();
    }

    private void sleepIfApply() {
        if (getExecutions() > 0) {
            int counter = 0;
            while (!hasEnded() && counter < sleepFraction) {
                try {
                    Thread.sleep(sleep / sleepFraction);
                    counter++;
                } catch (InterruptedException ie) {
                    log.warn("An interrupted exception has occurred.", ie);
                }
            }
        }
    }

    @Override
    protected boolean isPolicyCompleted() {
        return maxExecs >= 0 && getExecutions() >= maxExecs;
    }
}
