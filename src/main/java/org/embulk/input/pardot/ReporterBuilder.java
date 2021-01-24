package org.embulk.input.pardot;

import com.google.common.collect.ImmutableList;
import org.embulk.config.ConfigException;
import org.embulk.input.pardot.reporter.ReporterInterface;
import org.embulk.input.pardot.reporter.VisitorActivityReporter;
import org.embulk.spi.Column;

class ReporterBuilder
{
    private static final String OBJECT_TYPE_VISITOR_ACTIVITY = "visitor_activity";
    private ReporterBuilder()
    {
    }
    public static ReporterInterface create(PluginTask task)
    {
        if (task.getObjectType().equals(OBJECT_TYPE_VISITOR_ACTIVITY)) {
            return new VisitorActivityReporter(task);
        }
        throw new ConfigException("undefined object_type: " + task.getObjectType());
    }
}
