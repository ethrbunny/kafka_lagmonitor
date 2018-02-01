package com.cedexis.lagmonitortest;

import com.cedexis.ads_util.YamlConfigRunner;
import org.junit.Assert;
import org.junit.Test;


/**
 * Created on 3/24/17.
 */

// make sure we have valid yaml and it contains what it should
public class ConfigTest {
    @Test
    public void checkconfig() throws Exception {
        YamlConfigRunner yamlConfigRunner = new YamlConfigRunner("config.yaml");

        Assert.assertNotNull(yamlConfigRunner.getList("brokers"));
        Assert.assertNotNull(yamlConfigRunner.getList("items"));

        Assert.assertNotNull(yamlConfigRunner.getString("appname"));
        Assert.assertNotNull(yamlConfigRunner.getString("ddog_server"));
        Assert.assertNotNull(yamlConfigRunner.getString("ddog_project"));

        Assert.assertNotNull(yamlConfigRunner.getInt("timer_msec"));
    }
}
