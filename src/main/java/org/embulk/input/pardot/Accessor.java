package org.embulk.input.pardot;

import com.darksci.pardot.api.request.visitoractivity.VisitorActivityQueryRequest;
import com.darksci.pardot.api.response.visitoractivity.VisitorActivity;
import com.darksci.pardot.api.response.visitoractivity.VisitorActivityQueryResponse;
import com.google.common.base.CaseFormat;
import org.embulk.spi.Column;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Locale;

public class Accessor
{
    private final PluginTask task;
    private final VisitorActivity va;

    private final Logger logger = LoggerFactory.getLogger(Accessor.class);

    public Accessor(PluginTask task, VisitorActivity va)
    {
        this.task = task;
        this.va = va;
    }

    public String get(String name)
    {
        String methodName = "";
        try {
            switch (name) {
                case "campaign_id":
                    if (va.getCampaign() == null || va.getCampaign().getId() == null) {
                        return null;
                    }
                    return va.getCampaign().getId().toString();
                case "campaign_name":
                    if (va.getCampaign() == null || va.getCampaign().getName() == null) {
                        return null;
                    }
                    return va.getCampaign().getName();
                case "campaign_cost":
                    if (va.getCampaign() == null || va.getCampaign().getCost() == null) {
                        return null;
                    }
                    return va.getCampaign().getCost().toString();
                case "campaign_folder_id":
                    if (va.getCampaign() == null || va.getCampaign().getFolderId() == null) {
                        return null;
                    }
                    return va.getCampaign().getFolderId().toString();
                default:
            }
            methodName = "get" + CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, name);
            Class<VisitorActivity> clazz = (Class<VisitorActivity>) va.getClass();
            Method method = clazz.getDeclaredMethod(methodName);
            Object res =  method.invoke(va);
            if (res == null) {
                return null;
            }
            return res.toString();
        }
        catch (Exception e) {
            logger.warn("Accessor error: {} name: {}", e, name);
        }
        return null;
    }
}
