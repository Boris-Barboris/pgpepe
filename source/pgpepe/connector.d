module pgpepe.connector;

import core.time: Duration, seconds, msecs;
import std.random: uniform;

import vibe.core.log;
import vibe.core.core: sleep;
import vibe.core.sync: LocalTaskSemaphore;

public import dpeq.connection: BackendParams;
public import dpeq.result: QueryResult;

import pgpepe.constants;
import pgpepe.connection;
import pgpepe.exceptions;
import pgpepe.future;
import pgpepe.prepared;
import pgpepe.internal.pool;


@safe:


struct ConnectorSettings
{
    /// Backends that are accepting writes. Usually it's one master server.
    BackendParams[] rwBackends;
    /// Backends that are accepting only reads. Slave replication nodes.
    BackendParams[] roBackends;
    /// Each backend will be serviced by this many connections that are only
    /// issuing fast transactions.
    uint fastPoolSize = 4;
    /// Each backend will be serviced by this many connections that are only
    /// issuing slow transactions.
    uint slowPoolSize = 8;
    /// TCP connection timeout.
    Duration connectionTimeout = seconds(10);
    /// Plain SQL queries wich are ran right after each connection starts and
    /// it's default transaction type is set. Use this setting to setup locales
    /// or any other session-specific stuff.
    string[] conInitQueries;
    /// This isolation level is set on the connection start using
    /// SET SESSION CHARACTERISTICS AS TRANSACTION ... query.
    IsolationLevel defaultIsolation = READ_COMMITTED;
    /// Capacity of connection's pipeline queue. Equals to maximum number of queries
    /// sent but not yet received.
    uint queueCapacity = 256;
    /// Maximum number of concurrent transactions in progress.
    uint tsacQueueLimit = 2048;
    /// If true (default), arriving transactions on top of tsacQueueLimit will
    /// bounce with TransactionLimitReached exception thrown.
    bool tsacLimitThrow = true;
    /// Transaction retry limit for deadlock and serialization failure cases.
    int safeRetryLimit = 10;
    /// Transaction retry limit for connectivity failure cases.
    int sockRetryLimit = 1;
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
            logInfo("Creating connection pool for %s read/write backend",
                m_settings.rwBackends[i].host);
            rwPools[i] = new PgConnectionPool(
                conSettingsForBackend(m_settings.rwBackends[i], false),
                m_settings.fastPoolSize,
                m_settings.slowPoolSize);
        }
        m_tsacSemaphore = new LocalTaskSemaphore(m_settings.tsacQueueLimit);
    }

    private PgConnectionPool[] rwPools;
    private PgConnectionPool[] roPools;

    private uint m_tsacsRunning = 0;
    private LocalTaskSemaphore m_tsacSemaphore;

    /// total number of transactions in progress
    @property uint tsacsRunning() const { return m_tsacsRunning; }

    private void lockTransaction()
    {
        if (m_tsacsRunning >= m_settings.tsacQueueLimit && m_settings.tsacLimitThrow)
        {
            throw new TransactionLimitReached(
                "Concurrent transaction count limit reached");
        }
        m_tsacSemaphore.lock();
        m_tsacsRunning++;
    }

    private void unlockTransaction()
    {
        assert(m_tsacsRunning > 0);
        m_tsacsRunning--;
        m_tsacSemaphore.unlock();
    }

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

    private void withRetries(scope void delegate() @safe f)
    {
        int safeRetries = m_settings.safeRetryLimit;
        int sockRetries = m_settings.sockRetryLimit;
        Exception last;
        while (true)
        {
            if (safeRetries < 0 || sockRetries < 0)
            {
                assert(last !is null);
                logDebug("Retry limit reached, throwing %s ", last.classinfo.name);
                throw last;
            }
            try
            {
                f();
                return;
            }
            catch (PsqlSocketException ex)
            {
                logDiagnostic("Socket error %s in transaction: %s",
                    ex.classinfo.name, ex.msg);
                sockRetries--;
                last = ex;
            }
            catch (PsqlErrorResponseException ex)
            {
                safeRetries--;
                last = ex;
                if (ex.notice.code == "40001")
                {
                    logDiagnostic("serialization failure, retrying");
                    continue;
                }
                if (ex.notice.code == "40P01")
                {
                    logDiagnostic("deadlock detected, sleeping and retrying");
                    sleep(msecs(uniform(0, 10)));
                    continue;
                }
                throw ex;
            }
        }
    }

    /// Execute simple textual sql query
    QueryResult execute(string sql, TsacConfig tc = TSAC_FDEFAULT)
    {
        lockTransaction();
        scope(exit) unlockTransaction();
        QueryResult result;
        logDebug(`execute sql: "%s"`, sql);
        withRetries(() {
            PgConnectionPool cp = choosePool(tc);
            PgConnection c = cp.getConnection(tc.fast);
            PgFuture valFuture;
            PgFuture tsacFuture = c.runInTsac(tc, (scope PgConnection pgc) {
                valFuture = pgc.execute(sql);
            });
            if (valFuture.err)
                throw valFuture.err;
            if (tsacFuture.err)
                throw tsacFuture.err;
            result = valFuture.result;
        });
        logDebug(`received %d row block(s)`, result.blocks.length);
        return result;
    }

    /// Execute prepared statement
    QueryResult execute(scope AbstractPrepared p, bool describe = true, TsacConfig tc = TSAC_FDEFAULT)
    {
        lockTransaction();
        scope(exit) unlockTransaction();
        QueryResult result;
        logDebug(`execute prepared`);
        withRetries(() {
            PgConnectionPool cp = choosePool(tc);
            PgConnection c = cp.getConnection(tc.fast);
            PgFuture valFuture;
            PgFuture tsacFuture = c.runInTsac(tc, (scope PgConnection pgc) {
                valFuture = pgc.execute(p, describe);
            }, true);   // implicit transaction scope is ok here
            if (valFuture.err)
                throw valFuture.err;
            if (tsacFuture.err)
                throw tsacFuture.err;
            result = valFuture.result;
        });
        logDebug(`received %d row block(s)`, result.blocks.length);
        return result;
    }

    /// Run delegate in transaction
    void transaction(scope TsacDlg tsacBody, TsacConfig tc = TSAC_DEFAULT)
    {
        lockTransaction();
        scope(exit) unlockTransaction();
        withRetries(() {
            PgConnectionPool cp = choosePool(tc);
            PgConnection c = cp.getConnection(tc.fast);
            PgFuture tsacFuture = c.runInTsac(tc, tsacBody);
            if (tsacFuture.err)
                throw tsacFuture.err;
        });
    }
}