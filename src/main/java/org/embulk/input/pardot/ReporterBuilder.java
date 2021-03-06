package org.embulk.input.pardot;

import org.embulk.config.ConfigException;
import org.embulk.input.pardot.reporter.EmailReporter;
import org.embulk.input.pardot.reporter.EmailStatsReporter;
import org.embulk.input.pardot.reporter.ProspectReporter;
import org.embulk.input.pardot.reporter.ReporterInterface;
import org.embulk.input.pardot.reporter.VisitorActivityReporter;
import org.embulk.input.pardot.reporter.VisitorReporter;

class ReporterBuilder
{
    private static final String OBJECT_TYPE_VISITOR_ACTIVITY = "visitor_activity";
    private static final String OBJECT_TYPE_EMAIL_STATS = "email_stats";
    private static final String OBJECT_TYPE_EMAIL = "email";
    private static final String OBJECT_TYPE_PROSPECT = "prospect";
    private static final String OBJECT_TYPE_VISITOR = "visitor";
    private ReporterBuilder()
    {
    }
    public static ReporterInterface create(PluginTask task)
    {
        if (task.getObjectType().equals(OBJECT_TYPE_VISITOR_ACTIVITY)) {
            return new VisitorActivityReporter(task);
        }
        if (task.getObjectType().equals(OBJECT_TYPE_EMAIL_STATS)) {
            return new EmailStatsReporter(task);
        }
        if (task.getObjectType().equals(OBJECT_TYPE_EMAIL)) {
            return new EmailReporter(task);
        }
        if (task.getObjectType().equals(OBJECT_TYPE_PROSPECT)) {
            return new ProspectReporter(task);
        }
        if (task.getObjectType().equals(OBJECT_TYPE_VISITOR)) {
            return new VisitorReporter(task);
        }
        throw new ConfigException("undefined object_type: " + task.getObjectType());
    }
}
