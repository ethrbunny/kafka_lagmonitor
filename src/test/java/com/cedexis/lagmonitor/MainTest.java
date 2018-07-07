package com.cedexis.lagmonitor;

import com.datastax.driver.core.Session;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertTrue;

public class MainTest {
    @Test
    public void configTest() throws Exception {
        Main main = new Main();
        Map<String, Object> configMap = main.loadConfig();

        assertTrue(configMap.containsKey("timer_msec"));
        assertTrue(configMap.containsKey("reload_msec"));
        assertTrue(configMap.containsKey("brokers"));
        assertTrue(configMap.containsKey("cassandra"));

    }

}
