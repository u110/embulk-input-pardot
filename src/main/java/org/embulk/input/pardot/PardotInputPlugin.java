package org.embulk.input.pardot;

import com.darksci.pardot.api.PardotClient;
import com.darksci.pardot.api.config.Configuration;
import com.darksci.pardot.api.request.email.EmailStatsRequest;
import com.darksci.pardot.api.request.visitoractivity.VisitorActivityQueryRequest;
import com.darksci.pardot.api.request.visitoractivity.VisitorActivityReadRequest;
import com.darksci.pardot.api.response.email.EmailStatsResponse;
import com.darksci.pardot.api.response.visitoractivity.VisitorActivity;
import com.darksci.pardot.api.response.visitoractivity.VisitorActivityQueryResponse;
import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.darksci.pardot.api.ConfigurationBuilder;

import java.util.List;

public class PardotInputPlugin
        implements InputPlugin
{
    private final Logger logger = LoggerFactory.getLogger(PardotInputPlugin.class);

    @Override
    public ConfigDiff transaction(ConfigSource config,
                                  InputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema schema = task.getColumns().toSchema();
        int taskCount = 1;  // number of run() method calls

        return resume(task.dump(), schema, taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
                             Schema schema, int taskCount,
                             InputPlugin.Control control)
    {
        control.run(taskSource, schema, taskCount);
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
                        Schema schema, int taskCount,
                        List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TaskReport run(TaskSource taskSource,
                          Schema schema, int taskIndex,
                          PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        try {
            final ConfigurationBuilder configBuilder;
            if (task.getUserKey().isPresent()) {
                logger.warn("user_key will deprecate in spring 2021 see https://help.salesforce.com/articleView?id=000353746&type=1&mode=1&language=en_US&utm_source=techcomms&utm_medium=email&utm_campaign=eol");
                configBuilder = Configuration.newBuilder()
                        .withUsernameAndPasswordLogin(
                                task.getUserName(),
                                task.getPassword(),
                                task.getUserKey().toString()
                        );
            }
            else {
                logger.warn("use client_id / client_secret");
                if (task.getAppClientId().isPresent()
                        && task.getAppClientSecret().isPresent()
                        && task.getBusinessUnitId().isPresent()) {
                    configBuilder = Configuration.newBuilder()
                            .withSsoLogin(
                                    task.getUserName(),
                                    task.getPassword(),
                                    task.getAppClientId().toString(),
                                    task.getAppClientSecret().toString(),
                                    task.getBusinessUnitId().toString()
                            );
                }
                else {
                    throw new Exception("please set app_client_id, app_client_secret, business_unit_id");
                }
            }

            // Create config
            Configuration testConfig = configBuilder.build();
            PardotClient pardotClient = new PardotClient(configBuilder);
            VisitorActivityQueryRequest req = new VisitorActivityQueryRequest();
            VisitorActivityQueryResponse.Result res = pardotClient.visitorActivityQuery(req);
            logger.warn("total results: {}", res.getTotalResults().toString());
        }
        catch (Exception e) {
            logger.error(e.toString());
        }
        return Exec.newTaskReport();
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        return Exec.newConfigDiff();
    }

    public interface PluginTask
            extends Task
    {
        @Config("user_name")
        String getUserName();

        @Config("password")
        String getPassword();

        @Config("user_key")
        @ConfigDefault("null")
        Optional<String> getUserKey();

        @Config("app_client_id")
        @ConfigDefault("null")
        Optional<String> getAppClientId();

        @Config("app_client_secret")
        @ConfigDefault("null")
        Optional<String> getAppClientSecret();

        @Config("business_unit_id")
        @ConfigDefault("null")
        Optional<String> getBusinessUnitId();

        @Config("columns")
        SchemaConfig getColumns();
    }
}
