module pgpepe.connector;

import core.time: Duration, seconds;

public import dpeq.connection: BackendParams;
public import dpeq.result: QueryResult;

import pgpepe.constants;
import pgpepe.connection;
import pgpepe.exceptions;
import pgpepe.future;
import pgpepe.internal.pool;
import pgpepe.internal.taskqueue;


@safe:


struct ConnectorSettings
{
    /// Backends that are accepting writes. Usually it's one master server.
    BackendParams[] rwBackends;
    /// Backends that are accepting only reads. Slave replication nodes.
    BackendParams[] roBackends;
    /// Each backend will be serviced by this many connections that are only
    /// issuing fast transactions.
    size_t fastPoolSize = 2;
    /// Each backend will be serviced by this many connections that are
    /// issuing transactions in exclusive mode.
    size_t slowPoolSize = 4;
    /// TCP connection timeout.
    Duration connectionTimeout = seconds(10);
    /// Plain SQL queries wich are ran after each connection starts.
    string[] conInitQueries;
    /// This isolation level is set on the connection start using SET TRANSACTION command.
    IsolationLevel defaultIsolation = READ_COMMITTED;
    /// Capacity of connection's pipeline queue.
    size_t queueCapacity = 256;
}


final class PgConnector
{
    private immutable ConnectorSettings m_settings;

    this(immutable ConnectorSettings settings)
    {
        m_settings = settings;
        rwPools.length = m_settings.rwBackends.length;
        roPools.length = m_settings.roBackends.length;
        for (int i = 0; i < rwPools.length; i++)
        {
            rwPools[i] = new PgConnectionPool(
                conSettingsForBackend(m_settings.rwBackends[0], false),
                m_settings.fastPoolSize,
                m_settings.slowPoolSize);
        }
    }

    private PgConnectionPool[] rwPools;
    private PgConnectionPool[] roPools;

    private immutable(ConnectionSettings) conSettingsForBackend(
        ref immutable(BackendParams) bp, bool readOnly) const
    {
        return immutable ConnectionSettings(
            bp,
            m_settings.connectionTimeout,
            m_settings.conInitQueries,
            m_settings.defaultIsolation,
            readOnly,
            m_settings.queueCapacity
        );
    }

    private PgConnectionPool choosePool(ref const(TsacConfig) tconf)
    {
        // FIXME:
        return rwPools[0];
    }

    private static void withRetries(scope void delegate() @safe f)
    {
        int retryCounter = 0;
        Exception last;
        while (true)
        {
            if (retryCounter > 1)
            {
                assert(last !is null);
                throw last;
            }
            try
            {
                f();
                return;
            }
            catch (PsqlSocketException ex)
            {
                retryCounter++;
                last = ex;
            }
        }
    }

    QueryResult execute(string sql, ref in TsacConfig tc = TSAC_DEFAULT)
    {
        QueryResult result;
        withRetries(() {
            PgConnectionPool cp = choosePool(tc);
            PgConnection c = cp.getConnection(tc.fast);
            PgFuture valFuture;
            PgFuture tsacFuture = c.runInTsac(tc, (scope PgConnection pgc) {
                valFuture = pgc.execute(sql);
            });
            tsacFuture.await();
            if (valFuture.err)
                throw valFuture.err;
            if (tsacFuture.err)
                throw tsacFuture.err;
            result = valFuture.result;
        });
        return result;
    }
}