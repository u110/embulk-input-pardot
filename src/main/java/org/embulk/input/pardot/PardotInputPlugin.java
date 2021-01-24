package org.embulk.input.pardot;

import com.darksci.pardot.api.ConfigurationBuilder;
import com.darksci.pardot.api.PardotClient;
import com.darksci.pardot.api.config.Configuration;
import com.darksci.pardot.api.request.DateParameter;
import com.darksci.pardot.api.request.visitoractivity.VisitorActivityQueryRequest;
import com.google.common.collect.ImmutableList;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.input.pardot.accessor.AccessorInterface;
import org.embulk.input.pardot.reporter.ReporterInterface;
import org.embulk.spi.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PardotInputPlugin
        implements InputPlugin
{
    private static final Logger logger = LoggerFactory.getLogger(PardotInputPlugin.class);
    private ReporterInterface reporter;

    @Override
    public ConfigDiff transaction(ConfigSource config,
                                  InputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        reporter = ReporterBuilder.create(task);
        ImmutableList.Builder<Column> builder = reporter.createColumnBuilder();

        final Schema schema = new Schema(builder.build());
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
        final PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), schema, output);
        final PardotClient pardotClient = getClient(task);

        Integer totalResults;
        Integer rowIndex = 0;
        do {
            reporter.withOffset(rowIndex);
            reporter.executeQuery(pardotClient);
            if (reporter.hasResults()) {
                rowIndex += reporter.queryResultSize();
            }
            totalResults = reporter.getTotalResults();
            logger.info("total results: {}", totalResults);
            for (AccessorInterface accessor : reporter.accessors()) {
                schema.visitColumns(new ColVisitor(accessor, pageBuilder, task));
                pageBuilder.addRecord();
            }
            pageBuilder.flush();
            logger.info("fetched rows: {} total: {}", rowIndex, totalResults);
        }
        while(rowIndex < totalResults);

        pageBuilder.finish();
        return Exec.newTaskReport();
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        return Exec.newConfigDiff();
    }

    public static PardotClient getClient(PluginTask task)
    {
        final ConfigurationBuilder configBuilder;
        if (task.getUserKey().isPresent()) {
            logger.warn("user_key will deprecate in spring 2021 see https://help.salesforce.com/articleView?id=000353746&type=1&mode=1&language=en_US&utm_source=techcomms&utm_medium=email&utm_campaign=eol");
            configBuilder = Configuration.newBuilder()
                    .withUsernameAndPasswordLogin(
                            task.getUserName(),
                            task.getPassword(),
                            task.getUserKey().get()
                    );
            return new PardotClient(configBuilder);
        }
        logger.info("use client_id / client_secret");
        if (task.getAppClientId().isPresent()
                && task.getAppClientSecret().isPresent()
                && task.getBusinessUnitId().isPresent()) {
            configBuilder = Configuration.newBuilder()
                    .withSsoLogin(
                            task.getUserName(),
                            task.getPassword(),
                            task.getAppClientId().get(),
                            task.getAppClientSecret().get(),
                            task.getBusinessUnitId().get()
                    );
            return new PardotClient(configBuilder);
        }
        throw new ConfigException("please set app_client_id, app_client_secret, business_unit_id");
    }
}
