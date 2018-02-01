package com.cedexis.lagmonitor.datadog;

import com.cedexis.ads_util.YamlConfigRunner;
import com.timgroup.statsd.Event;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created on 9/28/16.
 */


/**
 * Created on 9/21/16.
 */
public class DDog {
    private static String hostname;
    private static StatsDClient statsd = null;
    private static DDog dDog;

    public static StatsDClient getDDog() {
        if (dDog == null) {
            dDog = new DDog();
        }

        return statsd;
    }

    private DDog() {
        YamlConfigRunner yamlConfigRunner = new YamlConfigRunner("config.yaml");

        if((hostname == null) || (hostname.isEmpty())) {
            try {
                InetAddress inetAddress = InetAddress.getLocalHost();
                hostname = inetAddress.getHostName();
            } catch (UnknownHostException exception) {
                System.out.println("Unable to determine host name");
                hostname = "";
            }
        }

        String appName = yamlConfigRunner.getString("appname");

        List<String> tagList = new ArrayList<>();
        tagList.add("app:" + appName);
        tagList.add("host:" + hostname);
        tagList.add("project:" + yamlConfigRunner.getString("ddog_project"));

        String[] tags = tagList.parallelStream().toArray(String[]::new);;

        if(statsd == null) {
            statsd = new NonBlockingStatsDClient(appName,
                    yamlConfigRunner.getString("ddog_server"),
                    8125,
                    tags);
        }
    }
}

