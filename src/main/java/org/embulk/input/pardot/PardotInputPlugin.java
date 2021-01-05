package org.embulk.input.pardot;

import com.darksci.pardot.api.PardotClient;
import com.darksci.pardot.api.config.Configuration;
import com.darksci.pardot.api.request.DateParameter;
import com.darksci.pardot.api.request.visitoractivity.VisitorActivityQueryRequest;
import com.darksci.pardot.api.response.visitoractivity.VisitorActivity;
import com.darksci.pardot.api.response.visitoractivity.VisitorActivityQueryResponse;
import com.google.common.collect.ImmutableList;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.darksci.pardot.api.ConfigurationBuilder;

import java.util.List;

public class PardotInputPlugin
        implements InputPlugin
{
    private static final Logger logger = LoggerFactory.getLogger(PardotInputPlugin.class);

    @Override
    public ConfigDiff transaction(ConfigSource config,
                                  InputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        ImmutableList.Builder<Column> columns = ImmutableList.builder();
        int i = 0;
        //<id> integer
        columns.add(new Column(i++, "id", Types.LONG));
        //<prospect_id> integer
        columns.add(new Column(i++, "prospect_id", Types.LONG));
        //<visitor_id> integer
        columns.add(new Column(i++, "visitor_id", Types.LONG));
        //<type> integer
        columns.add(new Column(i++, "type", Types.LONG));
        //<type_name> string
        columns.add(new Column(i++, "type_name", Types.STRING));
        //<details> string
        columns.add(new Column(i++, "details", Types.STRING));
        //<email_id> integer
        columns.add(new Column(i++, "email_id", Types.LONG));
        //<email_template_id> integer
        columns.add(new Column(i++, "email_template_id", Types.LONG));
        //<list_email_id> integer
        columns.add(new Column(i++, "list_email_id", Types.LONG));
        //<form_id> integer
        columns.add(new Column(i++, "form_id", Types.LONG));
        //<form_handler_id> integer
        columns.add(new Column(i++, "form_handler_id", Types.LONG));
        //<site_search_query_id> integer
        columns.add(new Column(i++, "site_search_query_id", Types.LONG));
        //<landing_page_id> integer
        columns.add(new Column(i++, "landing_page_id", Types.LONG));
        //<paid_search_ad_id> integer
        columns.add(new Column(i++, "paid_search_id", Types.LONG));
        //<multivariate_test_variation_id> integer
        columns.add(new Column(i++, "multivariate_test_variation_id", Types.LONG));
        //<visitor_page_view_id> integer
        columns.add(new Column(i++, "visitor_page_view_id", Types.LONG));
        //<file_id> integer
        columns.add(new Column(i++, "file_id", Types.LONG));
        //<custom_redirect_id> integer
        // columns.add(new Column(i++, "custom_redirect_id", Types.LONG)); // INFO not found on api client
        //<campaign> object
        // columns.add(new Column(i++, "campaign", Types.STRING));
        columns.add(new Column(i++, "campaign_id", Types.LONG));
        columns.add(new Column(i++, "campaign_name", Types.STRING));
        columns.add(new Column(i++, "campaign_cost", Types.LONG));
        columns.add(new Column(i++, "campaign_folder_id", Types.LONG));
        //<created_at> timestamp
        columns.add(new Column(i++, "created_at", Types.TIMESTAMP));
        //<updated_at> timestamp
        //columns.add(new Column(i++, "timestamp", Types.TIMESTAMP)); // INFO not found on api client

        final Schema schema = new Schema(columns.build());
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

        VisitorActivityQueryRequest req = getVisitorActivityQueryRequest(task);

        Integer totalResults;
        VisitorActivityQueryResponse.Result res;
        Integer rowIndex = 0;
        do {
            req = req.withOffset(rowIndex);
            // exec request
            res = pardotClient.visitorActivityQuery(req);
            if (res.getVisitorActivities() != null) {
                rowIndex += res.getVisitorActivities().size();
            }
            totalResults = res.getTotalResults();
            logger.info("total results: {}", totalResults);
            for (VisitorActivity va : res.getVisitorActivities()) {
                schema.visitColumns(new ColVisitor(new Accessor(task, va), pageBuilder, task));
                pageBuilder.addRecord();
            }
            pageBuilder.flush();
            logger.info("fetched rows: {} total: {}", rowIndex, totalResults);
        }
        while(rowIndex < totalResults);

        pageBuilder.finish();
        return Exec.newTaskReport();
    }

    private VisitorActivityQueryRequest getVisitorActivityQueryRequest(PluginTask task)
    {
        VisitorActivityQueryRequest req = new VisitorActivityQueryRequest();
        if (task.getFetchRowLimit().isPresent()) {
            req = req.withLimit(task.getFetchRowLimit().get());
        }
        if (task.getCreatedBefore().isPresent()) {
            req = req.withCreatedBefore(new DateParameter(task.getCreatedBefore().get()));
        }
        if (task.getCreatedAfter().isPresent()) {
            req = req.withCreatedAfter(new DateParameter(task.getCreatedAfter().get()));
        }
        if (task.getActivityTypeIds().isPresent()) {
            req = req.withActivityTypeIds(task.getActivityTypeIds().get());
        }
        if (task.getProspectIds().isPresent()) {
            req = req.withProspectIds(task.getProspectIds().get());
        }
        if (task.getSortKey().isPresent()) {
            req = req.withSortBy(task.getSortKey().get());
        }
        if (task.getSortOrder().isPresent()) {
            req = req.withSortOrder(task.getSortOrder().get());
        }
        return req;
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
