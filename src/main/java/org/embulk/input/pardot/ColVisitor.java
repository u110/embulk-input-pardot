package org.embulk.input.pardot;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.TimestampParser;

public class ColVisitor implements ColumnVisitor
{
    private final Accessor accessor;
    private final PluginTask task;
    private final PageBuilder pageBuilder;

    public ColVisitor(Accessor accessor, PageBuilder pageBuilder, PluginTask task)
    {
        this.accessor = accessor;
        this.pageBuilder = pageBuilder;
        this.task = task;
    }

    @Override
    public void booleanColumn(Column column)
    {
        String data = accessor.get(column.getName());
        if (data == null) {
            pageBuilder.setNull(column);
        }
        else {
            pageBuilder.setBoolean(column, Boolean.parseBoolean(data));
        }
    }

    @Override
    public void longColumn(Column column)
    {
        String data = accessor.get(column.getName());
        if (data == null) {
            pageBuilder.setNull(column);
        }
        else {
            pageBuilder.setLong(column, Long.parseLong(data));
        }
    }

    @Override
    public void doubleColumn(Column column)
    {
        try {
            String data = accessor.get(column.getName());
            pageBuilder.setDouble(column, Double.parseDouble(data));
        }
        catch (Exception e) {
            pageBuilder.setNull(column);
        }
    }

    @Override
    public void stringColumn(Column column)
    {
        String data = accessor.get(column.getName());
        if (data == null) {
            pageBuilder.setNull(column);
        }
        else {
            pageBuilder.setString(column, data);
        }
    }

    final TimestampParser parser = TimestampParser.of("%Y-%m-%dT%H:%M:%S.%L", "UTC");

    @Override
    public void timestampColumn(Column column)
    {
        String data = accessor.get(column.getName());
        if (data == null) {
            pageBuilder.setNull(column);
        }
        else {
            pageBuilder.setTimestamp(column, parser.parse(data));
        }
    }

    @Override
    public void jsonColumn(Column column)
    {
        // TODO:
        pageBuilder.setNull(column);
    }
}
