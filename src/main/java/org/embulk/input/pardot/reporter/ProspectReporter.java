package org.embulk.input.pardot.reporter;

import com.darksci.pardot.api.PardotClient;
import com.darksci.pardot.api.request.DateParameter;
import com.darksci.pardot.api.request.prospect.ProspectQueryRequest;
import com.darksci.pardot.api.response.prospect.Prospect;
import com.darksci.pardot.api.response.prospect.ProspectQueryResponse;
import com.google.common.collect.ImmutableList;
import org.embulk.input.pardot.PluginTask;
import org.embulk.input.pardot.accessor.AccessorInterface;
import org.embulk.input.pardot.accessor.ProspectAccessor;
import org.embulk.spi.Column;
import org.embulk.spi.type.Types;

import java.util.ArrayList;
import java.util.List;

public class ProspectReporter implements ReporterInterface
{
    private final PluginTask task;
    private final ProspectQueryRequest queryRequest;
    private ProspectQueryResponse.Result results;

    public ProspectReporter(PluginTask task)
    {
        this.task = task;
        this.queryRequest = buildQueryRequest();
    }

    public ImmutableList.Builder<Column> createColumnBuilder()
    {
        ImmutableList.Builder<Column> columns = ImmutableList.builder();
        int i = 0;
        // @see https://developer.pardot.com/kb/object-field-references/#prospect
        columns.add(new Column(i++, "id", Types.LONG));
        columns.add(new Column(i++, "first_name", Types.STRING));
        columns.add(new Column(i++, "last_name", Types.STRING));
        columns.add(new Column(i++, "created_at", Types.TIMESTAMP));
        columns.add(new Column(i++, "updated_at", Types.TIMESTAMP));
        return columns;
    }

    @Override
    public void withOffset(Integer offset)
    {
        this.queryRequest.withOffset(offset);
    }

    @Override
    public boolean hasResults()
    {
        return this.results != null;
    }

    @Override
    public void executeQuery(PardotClient client)
    {
        this.results = client.prospectQuery(queryRequest);
    }

    @Override
    public Integer queryResultSize()
    {
        if (this.results == null) {
            return 0;
        }
        return this.results.getProspects().size();
    }

    @Override
    public Integer getTotalResults()
    {
        if (this.results == null) {
            return 0;
        }
        return this.results.getTotalResults();
    }

    @Override
    public Iterable<? extends AccessorInterface> accessors()
    {
        List<ProspectAccessor> res = new ArrayList<>();
        for (Prospect p : this.results.getProspects()) {
            res.add(new ProspectAccessor(task, p));
        }
        Iterable<ProspectAccessor> itrable = res;
        return itrable;
    }

    @Override
    public void beforeExecuteQueries()
    {
        // do nothing
    }

    @Override
    public void afterExecuteQueries()
    {
        // do nothing
    }

    public ProspectQueryRequest buildQueryRequest()
    {
        ProspectQueryRequest req = new ProspectQueryRequest();
        if (task.getFetchRowLimit().isPresent()) {
            req = req.withLimit(task.getFetchRowLimit().get());
        }
        if (task.getCreatedBefore().isPresent()) {
            req = req.withCreatedBefore(new DateParameter(task.getCreatedBefore().get()));
        }
        if (task.getCreatedAfter().isPresent()) {
            req = req.withCreatedAfter(new DateParameter(task.getCreatedAfter().get()));
        }
        if (task.getUpdatedBefore().isPresent()) {
            req = req.withUpdatedBefore(new DateParameter(task.getUpdatedBefore().get()));
        }
        if (task.getUpdatedAfter().isPresent()) {
            req = req.withUpdatedAfter(new DateParameter(task.getUpdatedAfter().get()));
        }
        if (task.getSortKey().isPresent()) {
            req = req.withSortBy(task.getSortKey().get());
        }
        if (task.getSortOrder().isPresent()) {
            req = req.withSortOrder(task.getSortOrder().get());
        }
        return req;
    }
}
