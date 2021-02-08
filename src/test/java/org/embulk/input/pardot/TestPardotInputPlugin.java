package org.embulk.input.pardot;

import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.spi.InputPlugin;
import org.embulk.test.TestingEmbulk;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestPardotInputPlugin
{
    private static final String BASIC_RESOURCE_PATH = "org/embulk/input/pardot/";
    private static ConfigSource loadYamlResource(TestingEmbulk embulk, String fileName)
    {
        return embulk.loadYamlResource(BASIC_RESOURCE_PATH + fileName);
    }

    @Rule
    public TestingEmbulk embulk = TestingEmbulk.builder()
            .registerPlugin(InputPlugin.class, "pardot", PardotInputPlugin.class)
            .build();

    @Test
    public void test__getClient()
    {
        ConfigSource config = loadYamlResource(embulk, "config_test_empty_client_secret.yml");
        PluginTask task = config.loadConfig(PluginTask.class);
        try {
            PardotInputPlugin.getClient(task);
        }
        catch (ConfigException e) {
            assertEquals("please set app_client_id, app_client_secret, business_unit_id", e.getMessage());
            return;
        }
        assertTrue("Exception must be occurred", false);
    }

    @Test
    public void test__ColumnBuilder()
    {
        ConfigSource config = loadYamlResource(embulk, "config_undefined_object_type.yml");
        PluginTask task = config.loadConfig(PluginTask.class);
        try {
            ReporterBuilder.create(task);
        }
        catch (ConfigException e) {
            assertEquals("undefined object_type: fake_object_type", e.getMessage());
            return;
        }
        assertTrue("Exception must be occurred", false);
    }
}
