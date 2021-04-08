package org.embulk.input.pardot.reporter;

import com.darksci.pardot.api.PardotClient;
import com.darksci.pardot.api.request.email.EmailStatsRequest;
import com.darksci.pardot.api.request.visitoractivity.VisitorActivityQueryRequest;
import com.darksci.pardot.api.response.email.EmailStatsResponse;
import com.darksci.pardot.api.response.visitoractivity.VisitorActivity;
import com.darksci.pardot.api.response.visitoractivity.VisitorActivityQueryResponse;
import com.google.common.collect.ImmutableList;
import org.embulk.input.pardot.Client;
import org.embulk.input.pardot.PluginTask;
import org.embulk.input.pardot.accessor.AccessorInterface;
import org.embulk.input.pardot.accessor.EmailStatsAccessor;
import org.embulk.spi.Column;
import org.embulk.spi.type.Types;

import java.util.*;

public class EmailStatsReporter implements ReporterInterface
{
    private final PluginTask task;
    private List<Long> listEmailIds = new ArrayList<>();
    private List<EmailStatsResponse.Stats> results = new ArrayList<>();
    private Integer offset;

    public EmailStatsReporter(PluginTask task)
    {
        this.task = task;
    }

    public ImmutableList.Builder<Column> createColumnBuilder()
    {
        ImmutableList.Builder<Column> columns = ImmutableList.builder();
        int i = 0;
        // FIXME: add all params @see https://developer.pardot.com/kb/api-version-3/emails/#supported-operations_1
        columns.add(new Column(i++, "list_email_id", Types.LONG));
        columns.add(new Column(i++, "sent", Types.LONG));
        columns.add(new Column(i++, "delivered", Types.LONG));
        columns.add(new Column(i++, "total_clicks", Types.LONG));
        return columns;
    }

    @Override
    public void withOffset(Integer offset)
    {
        this.offset = offset;
    }

    @Override
    public boolean hasResults()
    {
        return this.results != null;
    }

    @Override
    public void executeQuery(PardotClient client)
    {
        int i = offset;
        while (i < listEmailIds.size()) {
            EmailStatsRequest queryRequest = new EmailStatsRequest().selectByListEmailId(listEmailIds.get(i));
            Optional<EmailStatsResponse.Stats> stats = client.emailStats(queryRequest);
            if (stats.isPresent()) {
                results.add(stats.get());
            }
            i++;
        }
    }

    @Override
    public Integer queryResultSize()
    {
        return results.size();
    }

    @Override
    public Integer getTotalResults()
    {
        if (this.listEmailIds == null) {
            return 0;
        }
        return this.listEmailIds.size();
    }

    @Override
    public Iterable<? extends AccessorInterface> accessors()
    {
        List<EmailStatsAccessor> res = new ArrayList<>();
        int i = 0;
        for (EmailStatsResponse.Stats item : this.results) {
            res.add(new EmailStatsAccessor(task, item, listEmailIds.get(i)));
            i++;
        }
        Iterable<EmailStatsAccessor> itrable = res;
        return itrable;
    }

    @Override
    public void beforeExecuteQueries()
    {
        // FIXME: list_email_id のリストを作成する

        // visitor activities からlist_email_idを取得する
        VisitorActivityReporter r = new VisitorActivityReporter(task);
        VisitorActivityQueryRequest req = r.buildQueryRequest();
        VisitorActivityQueryResponse.Result res = Client.getClient(task).visitorActivityQuery(req);

        Iterator<VisitorActivity> iterator = res.getVisitorActivities().iterator();
        while (iterator.hasNext()) {
            VisitorActivity va = iterator.next();
            this.listEmailIds.add(va.getListEmailId());
        }
        this.listEmailIds = new ArrayList<Long>(new HashSet<>(this.listEmailIds));
    }

    @Override
    public void afterExecuteQueries()
    {
        // do nothing
    }
}
