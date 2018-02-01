package com.cedexis.lagmonitortest;

import com.cedexis.ads_util.YamlConfigRunner;
import com.cedexis.lagmonitor.datadog.DDog;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.verifyNew;

/**
 * Created on 3/13/17.
 */

@RunWith(PowerMockRunner.class)
@PrepareForTest({DDog.class, InetAddress.class, NonBlockingStatsDClient.class})
public class DDogTest {
    @Mock
    YamlConfigRunner yamlConfigRunner;

    @Mock
    NonBlockingStatsDClient nonBlockingStatsDClient;

    String appName = "some app";
    String ddogServer = "some server";
    String hostname = "some host";
    String project = "some project";

    @Before
    public void setup() throws Exception {
        PowerMockito.whenNew(YamlConfigRunner.class).withAnyArguments().thenReturn(yamlConfigRunner);

        PowerMockito.when(yamlConfigRunner.getString("appname")).thenReturn(appName);
        PowerMockito.when(yamlConfigRunner.getString("ddog_server")).thenReturn(ddogServer);
        PowerMockito.when(yamlConfigRunner.getString("ddog_project")).thenReturn(project);

        nonBlockingStatsDClient = PowerMockito.mock(NonBlockingStatsDClient.class);
        PowerMockito.whenNew(NonBlockingStatsDClient.class).withAnyArguments().thenReturn(nonBlockingStatsDClient);

        InetAddress inetAddress = PowerMockito.mock(InetAddress.class);
        PowerMockito.mockStatic(InetAddress.class);
        PowerMockito.when(InetAddress.getLocalHost()).thenReturn(inetAddress);

        PowerMockito.when(inetAddress.getHostName()).thenReturn(hostname);
    }

    @After
    public void finish() throws Exception {
        verify(yamlConfigRunner).getString("appname");
        verify(yamlConfigRunner).getString("ddog_server");
        verify(yamlConfigRunner).getString("ddog_project");

        verifyNew(NonBlockingStatsDClient.class).withArguments(eq(appName), eq(ddogServer), eq(8125),
                eq("app:" + appName), eq("host:" + hostname), eq("project:" + project));
    }

    // make sure we're getting the same object each time
    @Test
    public void testStaticValues() throws Exception {
        StatsDClient sd0 = DDog.getDDog();
        StatsDClient sd1 = DDog.getDDog();

        Assert.assertEquals(sd0, sd1);

        verify(yamlConfigRunner, times(3)).getString(anyString());
    }

    @Ignore
    @Test
    public void cantfindhostname() throws Exception {
        PowerMockito.mockStatic(InetAddress.class);
        PowerMockito.when(InetAddress.getLocalHost()).thenThrow(new UnknownHostException());

        hostname = "";
        Whitebox.setInternalState(DDog.class, "dDog", null);
        StatsDClient sd0 = DDog.getDDog();
    }
}
