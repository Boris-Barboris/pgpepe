module pgpepe.connection;

import core.time: Duration, MonoTimeImpl, ClockType;

import vibe.core.net: TCPConnection, connectTCP;
import vibe.core.core: runTask;
import vibe.core.task: Task;
import vibe.core.stream;
import vibe.core.log;
import vibe.core.sync: TaskMutex;

import dpeq;
public import dpeq.authentication;
public import dpeq.transport;
public import dpeq.connection: SSLPolicy;

import pgpepe.constants;
import pgpepe.prepared;
import pgpepe.future;
import pgpepe.internal.taskqueue;



@safe:


struct ConnectionSettings
{
    /// Hostname or IP address of backend.
    string host;
    /// TCP port number.
    ushort port;
    /// SSL policy for dpeq connection.
    SSLPolicy sslPolicy;
    string user;
    string databaseName;
    /// Authentication plugin, password or some other
    IPSQLAuthenticator authenticator;
    /// TCP socket timeouts
    Duration connectTimeout;
    Duration readTimeout;
    Duration writeTimeout;
    /// Default isolation level to set for the newly created connection.
    IsolationLevel defaultIsolation;
    /// Array of simple string SQL queries to in the end of connection initialization.
    string[] conInitQueries;
    /// If the connection is to be set up as readonly.
    bool readonly;
    /// Query result capacity.
    uint queueCapacity;
}

enum ConnectionState: byte
{
    uninitialized,
    connecting,
    active,
    closed
}


/** Transaction body delegate signature. Run queries while exclusively owning
connection object and return (commit is issued by pgpepe on return, or
rollback if something was thrown). */
alias TsacDlg = void delegate(scope PgConnection) @trusted;


final class PgConnection
{
    private immutable ConnectionSettings m_settings;

    private static final class VibeCoreSocket
    {
        private TCPConnection m_con;

        this(string host, ushort port, Duration timeout)
        {
            try
            {
                m_con = connectTCP(host, port, null, 0, timeout);
                m_con.keepAlive = true;
                m_con.readTimeout = seconds(5);
            }
            catch (Exception ex)
            {
                throw new PsqlSocketException("Cannot connect to Psql", ex);
            }
        }

        void close() nothrow
        {
            m_con.close();
        }

        size_t send(const(ubyte)[] buf)
        {
            try
            {
                return m_con.write(buf, IOMode.all);
            }
            catch (Exception ex)
            {
                throw new PsqlSocketException("Error on socket write", ex);
            }
        }

        size_t receive(scope ubyte[] buf)
        {
            try
            {
                return m_con.read(buf, IOMode.all);
            }
            catch (Exception ex)
            {
                throw new PsqlSocketException("Error on socket read", ex);
            }
        }

        private @property readTimeout(in Duration rhs)
        {
            m_con.readTimeout = rhs;
        }
    }

    package alias DpeqConT = PSQLConnection!(VibeCoreSocket, nop_logger, logError);
    private alias CoarseTime = MonoTimeImpl!(ClockType.coarse);

    private DpeqConT m_con;
    private CoarseTime m_lastRelease;
    private ConnectionState m_state;

    @property ConnectionState state() const { return m_state; }

    package void markReleaseTime()
    {
        m_lastRelease = CoarseTime.currTime();
    }

    package @property Duration timeSinceLastRelease() const
    {
        return CoarseTime.currTime() - m_lastRelease;
    }

    package this(immutable ConnectionSettings settings)
    {
        m_settings = settings;
        m_tsacMutex = new TaskMutex();
        m_resultQueue = TaskQueue!PgFuture(settings.queueCapacity);
    }

    package void open()
    {
        assert(m_state == ConnectionState.uninitialized);
        m_tsacMutex.lock();
        scope (exit) m_tsacMutex.unlock();
        if (m_state == ConnectionState.uninitialized)
            establishConnection();
    }

    package void close() nothrow
    {
        if (m_state != ConnectionState.closed)
        {
            m_state = ConnectionState.closed;
            if (m_con)
            {
                logInfo("Closing connection to %s", m_settings.backendParam.host);
                m_con.terminate(false);
            }
            // notify the reader task that it's time to die
            m_resultQueue.pushBack(null);
        }
    }

    private void establishConnection()
    {
        assert(m_state == ConnectionState.uninitialized);
        m_state = ConnectionState.connecting;
        scope(failure)
        {
            logError("Failed to establish connection to %s", m_settings.backendParam.host);
            m_state = ConnectionState.closed;
            if (m_con)
            {
                m_con.terminate(false);
                m_con = null;
            }
        }
        logDebug("Connecting to %s...", m_settings.backendParam.host);
        m_con = new DpeqConT(m_settings.backendParam, m_settings.connectionTimeout);
        logInfo("Connected to %s", m_settings.backendParam.host);
        initializeConnection();
        m_lastRelease = CoarseTime.currTime();
        // start reader task
        m_readerTask = runTask(&readerTaskProc);
        // we are ready to accept transactions
        m_state = ConnectionState.active;
    }

    private void initializeConnection()
    {
        // set session default transaction mode
        logDebug("setting default transaction mode for %s",
            m_settings.backendParam.host);
        m_con.putQueryMessage(
            ["SET SESSION CHARACTERISTICS AS TRANSACTION ",
            beginTsacStr(
                TsacConfig(m_settings.defaultIsolation,
                    m_settings.readonly, true, false))]);
        m_con.flush();
        m_con.pollMessages(null);
        // confirm that it works
        logDebug("validating transaction settings for %s",
                m_settings.backendParam.host);
        m_con.putQueryMessage("BEGIN");
        m_con.putQueryMessage("COMMIT");
        m_con.flush();
        m_con.pollMessages(null);
        m_con.pollMessages(null);
        // perform all initialization queries
        foreach (sql; m_settings.conInitQueries)
        {
            logDebug("running conInitQuery: %s", sql);
            m_con.putQueryMessage(sql);
            m_con.flush();
            m_con.pollMessages(null);
        }
        logDebug("Connection initialized");
    }

    // mutex that guards connection's write buffer and only lets in one
    // transaction
    private TaskMutex m_tsacMutex;

    private TaskQueue!PgFuture m_resultQueue;
    private Task m_readerTask;

    @property size_t queueLength() const { return m_resultQueue.length; }

    private void readerTaskProc()
    {
        while (true)
        {
            PgFuture future = m_resultQueue.popFront(); // blocks fiber
            try
            {
                if (future !is null)
                    future.complete(getQueryResults(m_con));
                else
                {
                    if (m_state == ConnectionState.closed)
                        return;
                    m_con.pollMessages(null);
                }
            }
            catch (PsqlSocketException ex)
            {
                logInfo("Connection to %s assumed to be closed",
                    m_settings.backendParam.host);
                if (m_state != ConnectionState.closed)
                {
                    m_state = ConnectionState.closed;
                    m_con.terminate(false);
                }
                if (future !is null)
                    future.complete(ex);
                // windup remaining waiters.
                while (m_resultQueue.length > 0)
                {
                    future = m_resultQueue.popFront();
                    if (future !is null)
                        future.complete(ex);
                }
                // Absence of new waiters after this point is guaranteed by close() and terminate().
                return;
            }
            catch (Exception ex)
            {
                if (future !is null)
                {
                    logDebug("%s caught in readerTask: %s", ex.classinfo.name, ex.msg);
                    future.complete(ex);
                }
                else
                    logError("Ignoring exception %s %s, no accepting future",
                        ex.classinfo.name, ex.msg);
            }
        }
    }

    private int m_tsacsBlocked = 0;
    // number of transactions currently blocked on m_tsacMutex or holding it
    package @property int tsacsBlocked() const { return m_tsacsBlocked; }

    // returns true if explicit BEGIN was sent to backend
    private bool beginTsac(in TsacConfig tc, bool allowImplicit)
    {
        logDebug("begin transaction");
        if (tc.isolation == m_settings.defaultIsolation &&
            tc.readonly == m_settings.readonly && !tc.deferrable)
        {
            if (allowImplicit)
                return false;
            m_con.putQueryMessage("BEGIN");
        }
        else
            m_con.putQueryMessage(["BEGIN ", beginTsacStr(tc)]);
        m_con.flush();
        // !!!!! Danger: we ignore begin's result. We assume we checked
        // permissions correctly in initializeConnection.
        m_resultQueue.pushBack(null);
        return true;
    }

    private static PgFuture g_successFuture;

    static this()
    {
        g_successFuture = new PgFuture();
        g_successFuture.complete(QueryResult.init);
    }

    // returns future to commit result
    package PgFuture runInTsac(in TsacConfig tc, scope TsacDlg tsacBody,
        bool allowImplicit = false) @trusted
    {
        assert(tc.readonly || !m_settings.readonly,
            "write transaction on read-only connection");
        m_tsacsBlocked++;
        if (m_tsacsBlocked > 1)
            logDebug("Blocking on connection m_tsacMutex behind %d contenders", m_tsacsBlocked - 1);
        m_tsacMutex.lock();
        scope (exit)
        {
            m_tsacsBlocked--;
            assert(m_tsacsBlocked >= 0);
            m_tsacMutex.unlock();
        }
        if (m_state != ConnectionState.active)
            throw new PsqlSocketException("connection is closed");  // FIXME
        bool explicitTsac;
        try
        {
            explicitTsac = beginTsac(tc, allowImplicit);
            tsacBody(this);
            if (!explicitTsac)
            {
                logDebug("implicit commit");
                return g_successFuture;
            }
            logDebug("explicit commit");
            m_con.putQueryMessage("COMMIT");
            m_con.flush();
            PgFuture commitFuture = new PgFuture();
            m_resultQueue.pushBack(commitFuture);
            return commitFuture;
        }
        catch (PsqlSocketException ex)
        {
            logError("socket error in transaction: %s", ex.msg);
            close();
            throw ex;
        }
        catch (Throwable t)
        {
            if (!explicitTsac)
            {
                logDiagnostic("%s rethrown from implicit rollback: %s",
                    t.classinfo.name, t.msg);
                throw t;
            }
            if (m_con.isOpen)
            {
                logDiagnostic("%s caught, explicit rollback: %s", t.classinfo.name, t.msg);
                try
                {
                    m_con.putQueryMessage("ROLLBACK");
                    m_con.flush();
                    m_resultQueue.pushBack(null);
                }
                catch (Exception ex)
                {
                    logDiagnostic("Unexpected error %s during explicit rollback: %s. Closing connection.",
                        ex.classinfo.name, ex.msg);
                    close();
                }
            }
            else
            {
                close();
                throw new PsqlSocketException("Unable to rollback using closed connection", t);
            }
            throw t;
        }
    }

    /// Send simple query
    PgFuture execute(string sql, PgFuture f = null)
    {
        m_con.putQueryMessage(sql);
        m_con.flush();
        if (f is null)
            f = new PgFuture();
        m_resultQueue.pushBack(f);
        return f;
    }

    private string[ulong] m_psCache;

    /// Execute prepared statement
    PgFuture execute(scope BasePrepared p, bool describe = true, PgFuture f = null)
    {
        if (p.named)
        {
            string* psName = p.hash in m_psCache;
            if (psName is null)
            {
                logDebug("Prepared statement cache miss");
                string newName = m_con.getNewPreparedName();
                p.parse(m_con, newName);
                // we need to wait for parse, because it may fail
                m_con.sync();
                m_con.flush();
                auto parseFuture = new PgFuture();
                m_resultQueue.pushBack(parseFuture);
                parseFuture.throwIfErr();
                m_psCache[p.hash] = newName;
                p.bind(m_con, newName, "");
            }
            else
            {
                logDebug("Prepared statement cache hit");
                p.bind(m_con, *psName, "");
            }
        }
        else
        {
            p.parse(m_con, "");
            p.bind(m_con, "", "");
        }
        if (describe)
            m_con.putDescribeMessage(StmtOrPortal.portal, "");
        m_con.putExecuteMessage("");
        m_con.sync();
        m_con.flush();
        if (f is null)
            f = new PgFuture();
        m_resultQueue.pushBack(f);
        return f;
    }
}