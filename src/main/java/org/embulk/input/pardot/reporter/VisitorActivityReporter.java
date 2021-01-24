package org.embulk.input.pardot.reporter;

import com.google.common.collect.ImmutableList;
import org.embulk.spi.Column;
import org.embulk.spi.type.Types;

public class VisitorActivityReporter
{
    private VisitorActivityReporter()
    {}

    public static ImmutableList.Builder<Column> createColumnBuilder()
    {
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
        return columns;
    }
}
