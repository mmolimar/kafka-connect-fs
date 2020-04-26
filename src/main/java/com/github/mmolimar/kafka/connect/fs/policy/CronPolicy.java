package com.github.mmolimar.kafka.connect.fs.policy;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Map;

public class CronPolicy extends AbstractPolicy {

    private static final Logger log = LoggerFactory.getLogger(CronPolicy.class);

    private static final String CRON_POLICY_PREFIX = FsSourceTaskConfig.POLICY_PREFIX + "cron.";

    public static final String CRON_POLICY_EXPRESSION = CRON_POLICY_PREFIX + "expression";
    public static final String CRON_POLICY_END_DATE = CRON_POLICY_PREFIX + "end_date";

    private final Time time;
    private ExecutionTime executionTime;
    private Date endDate;

    public CronPolicy(FsSourceTaskConfig conf) throws IOException {
        super(conf);
        this.time = new SystemTime();
    }

    @Override
    protected void configPolicy(Map<String, Object> customConfigs) {
        try {
            if (customConfigs.get(CRON_POLICY_END_DATE) != null &&
                    !customConfigs.get(CRON_POLICY_END_DATE).toString().equals("")) {
                endDate = Date.from(LocalDateTime.parse(customConfigs.get(CRON_POLICY_END_DATE).toString().trim())
                        .atZone(ZoneId.systemDefault()).toInstant());
            }
            executionTime = ExecutionTime.forCron(
                    new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ))
                            .parse(customConfigs.get(CRON_POLICY_EXPRESSION).toString())
            );
        } catch (DateTimeException dte) {
            throw new ConfigException(CRON_POLICY_END_DATE + " property must have a proper value. Got: '" +
                    customConfigs.get(CRON_POLICY_END_DATE) + "'.");
        } catch (IllegalArgumentException iae) {
            throw new ConfigException(CRON_POLICY_EXPRESSION + " property must have a proper value. Got: '" +
                    customConfigs.get(CRON_POLICY_EXPRESSION) + "'.");
        }
    }

    @Override
    protected void preCheck() {
        executionTime.timeToNextExecution(ZonedDateTime.now())
                .ifPresent(next -> time.sleep(next.toMillis()));
    }

    @Override
    protected boolean isPolicyCompleted() {
        return (endDate != null &&
                endDate.before(Date.from(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant()))) ||
                !executionTime.timeToNextExecution(ZonedDateTime.now()).isPresent();
    }
}
