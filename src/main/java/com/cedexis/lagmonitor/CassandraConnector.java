package com.cedexis.lagmonitor;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import java.net.InetAddress;
import java.util.Collection;

public class CassandraConnector {

    private Cluster cluster;

    private static Session session = null;

    private static int PORT = 9042;

    public void connect(Collection<InetAddress> addresses) {
        Cluster.Builder b = Cluster.builder().addContactPoints(addresses);
        b.withPort(PORT);

        cluster = b.build();

        session = cluster.connect();
    }

    public static Session getSession() {
        return session;
    }

    public void close() {
        session.close();
        cluster.close();
    }
}

