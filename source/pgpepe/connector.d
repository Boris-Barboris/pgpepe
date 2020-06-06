module pgpepe.connector;

import core.time: Duration, seconds, msecs, minutes;
import std.conv: to;
import std.random: uniform;

import vibe.core.log;
import vibe.core.core: sleep;
import vibe.core.sync: LocalTaskSemaphore;


public import dpeq.transport: StdConnectParameters;
public import dpeq.connection: SSLPolicy;

import pgpepe.constants;
import pgpepe.connection;
import pgpepe.exceptions;
import pgpepe.future;
import pgpepe.prepared;
import pgpepe.internal.pool;


@safe:


struct ConnectorSettings
{
    /// Backends that are accepting read-write transactions in round-robin fashion.
    /// Usually it's just a VIP of a master server.
    StdConnectParameters[] backends;
    /// Optionally, you can specify backends that are accepting all read-only transactions.
    /// If left emtpy, 'backends' are used.
    StdConnectParameters[] roBackends;
    /// SSL policy, applied to all connections.
    SSLPolicy sslPolicy;
    /// Each backend will be serviced by this many connections that are only
    /// issuing fast transactions.
    int fastPoolSize = 4;
    /// Each backend will be serviced by this many connections that are only
    /// issuing slow transactions.
    int slowPoolSize = 8;
    /// TCP connection timeout.
    Duration connectionTimeout = seconds(5);
    /// Connection is assumed to be stale after this long 'parking' time in the pool
    /// and will be lazily reopened for next transaction.
    Duration connectionStaleAfter = minutes(20);
    /// This isolation level is set on the connection start using
    /// SET SESSION CHARACTERISTICS AS TRANSACTION ... query.
    IsolationLevel defaultIsolation = READ_COMMITTED;
    /// Plain SQL queries wich are ran right after each connection is initialized and
    /// it's default transaction type is set. Use this setting to setup locales
    /// or any other session-specific parameters.
    string[] conInitQueries;
    /// Capacity of per-connection query queue. Pgpepe will pipeline queries
    /// and block the querying fiber when this number of unprocessed queries is reached.
    int conQueueCapacity = 32;
    /// Maximum number of transactions that have entered the connector
    /// object, both queued and in-progress.
    int tsacLimit = 1024;
    /// If true (default), arriving transactions on top of tsacQueueLimit will
    /// bounce with TransactionLimitException exception thrown. If false,
    /// the caller fibers will block.
    bool tsacLimitThrow = true;
    /// Default number of retries for deadlock and serialization failure errors.
    /// 0 - no retries.
    int safeRetryLimit = 3;
    /// Default number of retries for transport-level errors. Only correct for idempotent
    /// transactions. 0 - no retries.
    int transportRetryLimit = 0;
}


/// System of connection pools and queues that you can issue transactions to.
final class PgConnector
{
    private const ConnectorSettings m_settings;

    this(const ConnectorSettings settings)
    {
        m_settings = settings;
        rwPools.length = m_settings.backends.length;
        roPools.length = m_settings.roBackends.length;
        // rw
        for (int i = 0; i < rwPools.length; i++)
        {
            logInfo("Creating connection pool for %s read/write backend",
                m_settings.backends[i].host);
            rwPools[i] = new PgConnectionPool(
                conSettingsForBackend(m_settings.backends[i], false),
                m_settings.fastPoolSize,
                m_settings.slowPoolSize);
        }
        // ro
        for (int i = 0; i < roPools.length; i++)
        {
            logInfo("Creating connection pool for %s read-only backend",
                m_settings.roBackends[i].host);
            roPools[i] = new PgConnectionPool(
                conSettingsForBackend(m_settings.roBackends[i], true),
                m_settings.fastPoolSize,
                m_settings.slowPoolSize);
        }
        m_tsacSemaphore = new LocalTaskSemaphore(m_settings.tsacLimit);
    }

    /// Execute simple textual sql query in a transaction.
    QueryResult execute(string sql, TsacConfig tc = TSAC_FDEFAULT)
    {
        lockTransaction();
        scope(exit) unlockTransaction();
        QueryResult result;
        logDebug(`execute sql: "%s"`, sql);
        withRetries(() {
            PgConnectionPool cp = choosePool(tc);
            PgConnection c = cp.lock(tc.fast);
            scope(exit) cp.unlock(tc.fast);
            PgFuture valFuture;
            PgFuture tsacFuture = c.runInTsac(tc, (scope PgConnection pgc) {
                valFuture = pgc.execute(sql);
            });
            valFuture.throwIfErr();
            tsacFuture.throwIfErr();
            result = valFuture.result;
        });
        if (result.blocks.length > 0)
            logDebug(`received %d rows in first row block`, result.blocks[0].dataRows.length);
        return result;
    }

    /// Execute prepared statement
    QueryResult execute(scope BasePrepared p, bool describe = true, TsacConfig tc = TSAC_FDEFAULT)
    {
        lockTransaction();
        scope(exit) unlockTransaction();
        QueryResult result;
        logDebug(`execute prepared`);
        withRetries(() {
            PgConnectionPool cp = choosePool(tc);
            PgConnection c = cp.lock(tc.fast);
            scope(exit) cp.unlock(tc.fast);
            PgFuture valFuture;
            PgFuture tsacFuture = c.runInTsac(tc, (scope PgConnection pgc) {
                valFuture = pgc.execute(p, describe);
            }, true);   // implicit transaction scope is ok here
            valFuture.throwIfErr();
            tsacFuture.throwIfErr();
            result = valFuture.result;
        });
        if (result.blocks.length > 0)
            logDebug(`received %d rows in first row block`, result.blocks[0].dataRows.length);
        return result;
    }

    /// Run delegate in transaction
    void transaction(scope TsacDlg tsacBody, TsacConfig tc = TSAC_DEFAULT)
    {
        lockTransaction();
        scope(exit) unlockTransaction();
        withRetries(() {
            PgConnectionPool cp = choosePool(tc);
            PgConnection c = cp.lock(tc.fast);
            scope(exit) cp.unlock(tc.fast);
            PgFuture tsacFuture = c.runInTsac(tc, tsacBody);
            tsacFuture.throwIfErr();
        });
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
            throw new TransactionLimitException(
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

    private ConnectionSettings conSettingsForBackend(
        ref const(StdConnectParameters) bp, bool readOnly) const
    {
        return ConnectionSettings(
            bp,
            m_settings.connectionTimeout,
            m_settings.conInitQueries,
            m_settings.defaultIsolation,
            readOnly,
            m_settings.queueCapacity
        );
    }

    private int rwRR = 0;
    private int roRR = 0;

    // round-robin scheduling between backends
    private PgConnectionPool choosePool(in TsacConfig tconf)
    {
        PgConnectionPool res;
        if (tconf.readonly)
        {
            if (roPools.length == 0)
                assert(0, "No readonly backends");
            res = roPools[roRR];
            roRR = ((roRR + 1) % roPools.length).to!int;
        }
        else
        {
            if (rwPools.length == 0)
                assert(0, "No readonly backends");
            res = rwPools[rwRR];
            rwRR = ((rwRR + 1) % rwPools.length).to!int;
        }
        return res;
    }

    private void withRetries(scope void delegate() @safe dlg)
    {
        int safeRetries = m_settings.safeRetryLimit;
        int sockRetries = m_settings.transportRetryLimit;
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
                dlg();
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
                    int toSleep = uniform(0, 10);
                    if (toSleep > 0)
                        sleep(msecs(toSleep));
                    continue;
                }
                throw ex;
            }
        }
    }
}