package org.embulk.input.pardot;

import com.google.common.collect.ImmutableList;
import org.embulk.config.ConfigException;
import org.embulk.input.pardot.reporter.VisitorActivityReporter;
import org.embulk.spi.Column;

class ColumnBuilder
{
    private static final String OBJECT_TYPE_VISITOR_ACTIVITY = "visitor_activity";
    private ColumnBuilder()
    {
    }
    public static ImmutableList.Builder<Column> create(PluginTask task)
    {
        if (task.getObjectType().equals(OBJECT_TYPE_VISITOR_ACTIVITY)) {
            return VisitorActivityReporter.createColumnBuilder();
        }
        throw new ConfigException("undefined object_type: " + task.getObjectType());
    }
}
