package com.github.mmolimar.kafka.connect.fs.policy;

import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import org.apache.kafka.common.config.ConfigException;

import java.io.IOException;
import java.util.Map;

public class SleepyPolicy extends AbstractPolicy {

    public static final String SLEEPY_POLICY_SLEEP_MS = FsSourceTaskConfig.POLICY_PREFIX_CUSTOM + "sleep";
    public static final String SLEEPY_POLICY_SLEEP_FRACTION_MS = SLEEPY_POLICY_SLEEP_MS + ".fraction";
    public static final String SLEEPY_POLICY_MAX_EXECS = FsSourceTaskConfig.POLICY_PREFIX_CUSTOM + "max.executions";

    private static final int DEFAULT_SLEEP_FRACTION = 10;

    private long sleep;
    private long sleepFraction;
    private long maxExecs;

    public SleepyPolicy(FsSourceTaskConfig conf) throws IOException {
        super(conf);
    }

    @Override
    protected void configPolicy(Map<String, Object> customConfigs) {
        try {
            this.sleep = Long.valueOf((String) customConfigs.get(SLEEPY_POLICY_SLEEP_MS));
        } catch (NumberFormatException nfe) {
            throw new ConfigException(SLEEPY_POLICY_SLEEP_MS + " property is required and must be a number(long). Got: " +
                    customConfigs.get(SLEEPY_POLICY_SLEEP_MS));
        }
        if (customConfigs.get(SLEEPY_POLICY_MAX_EXECS) != null) {
            try {
                this.maxExecs = Long.valueOf((String) customConfigs.get(SLEEPY_POLICY_MAX_EXECS));
            } catch (NumberFormatException nfe) {
                throw new ConfigException(SLEEPY_POLICY_MAX_EXECS + " property must be a number(long). Got: " +
                        customConfigs.get(SLEEPY_POLICY_MAX_EXECS));
            }
        }
        if (customConfigs.get(SLEEPY_POLICY_SLEEP_FRACTION_MS) != null) {
            try {
                this.sleepFraction = Long.valueOf((String) customConfigs.get(SLEEPY_POLICY_SLEEP_FRACTION_MS));
            } catch (NumberFormatException nfe) {
                throw new ConfigException(SLEEPY_POLICY_SLEEP_FRACTION_MS + " property must be a number(long). Got: " +
                        customConfigs.get(SLEEPY_POLICY_SLEEP_FRACTION_MS));
            }
        } else {
            this.sleepFraction = DEFAULT_SLEEP_FRACTION;
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
                    throw new RuntimeException(ie);
                }
            }
        }
    }

    @Override
    protected boolean isPolicyCompleted() {
        return maxExecs >= 0 && getExecutions() >= maxExecs;
    }
}
