package org.embulk.input.pardot.reporter;

import com.darksci.pardot.api.PardotClient;
import com.google.common.collect.ImmutableList;
import org.embulk.input.pardot.accessor.AccessorInterface;
import org.embulk.spi.Column;

public interface ReporterInterface
{
    ImmutableList.Builder<Column> createColumnBuilder();

    void withOffset(Integer rowIndex);

    boolean hasResults();

    void executeQuery(PardotClient client);

    Integer queryResultSize();

    Integer getTotalResults();

    Iterable<? extends AccessorInterface> accessors();

    void beforeExecuteQueries();
    void afterExecuteQueries();
}
