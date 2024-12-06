package mvsync.db;


import com.datastax.oss.driver.api.core.CqlSession;
import mvsync.MVSyncSettings;
import mvsync.MvSync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import javax.annotation.Nullable;
import java.net.InetSocketAddress;

public class CassandraClient {
    private static Boolean initializedFromSettings = false;
    @Nullable private static CqlSession session = null;
    private static Logger log = LoggerFactory.getLogger(CassandraClient.class);
    @Nullable private MVSyncSettings mvSyncSettings = null;

    public void initCassandra(MVSyncSettings mvSyncSettings) throws Exception {
        if (!initializedFromSettings) {
            synchronized (MvSync.class) {
                if (!initializedFromSettings) {
                    this.mvSyncSettings = mvSyncSettings;
                    log.info("Initializing from cassandra client.");
                    session = getSession();
                    String clusterName = String.valueOf(session.getMetadata().getClusterName());
                    log.info("CassandraClient is initialized for {}", clusterName);
                    initializedFromSettings = true;
                }
            }
        }
    }

    private CqlSession buildSession() {
        session = CqlSession.builder().
                addContactPoint(new InetSocketAddress(mvSyncSettings.getCassandraConnectionHost(), Integer.parseInt(mvSyncSettings.getCassandraConnectionPort()))).
                withAuthCredentials(mvSyncSettings.getCassandraUserName() == null? "cassandra" : mvSyncSettings.getCassandraUserName(),
                        mvSyncSettings.getCassandraPassword() == null ? "cassandra" : mvSyncSettings.getCassandraPassword()).
                withLocalDatacenter(mvSyncSettings.getCassandraDatacenter()).
                build();
        return session;
    }

    public CqlSession getSession() throws Exception {
        if (session == null) {
            synchronized (MvSync.class) {
                buildSession();
                if (session != null) {
                    return session;
                }
                throw new Exception("Could not initialize Cassandra session");
            }
        }
        return session;
    }

    @VisibleForTesting
    public static void setSession(@Nullable CqlSession newSession) {
        session = newSession;
    }

    public void close() {
        if (session != null) {
            session.close();
        }
    }
}
