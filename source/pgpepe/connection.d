module pgpepe.connection;

import core.time: Duration, MonoTime;

import vibe.core.net: TCPConnection, connectTCP;
import vibe.core.core: runTask;
import vibe.core.task: Task;
import vibe.core.log;
import vibe.core.stream: IOMode;
import vibe.core.sync: TaskMutex;

import dpeq;

import pgpepe.constants;
import pgpepe.prepared;
import pgpepe.future;
import pgpepe.internal.taskqueue;



@safe:


struct ConnectionSettings
{
    BackendParams backendParam;
    Duration connectionTimeout;
    string[] conInitQueries;
    IsolationLevel defaultIsolation;
    bool readonly;
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
alias TsacDlg = void delegate(scope PgConnection) @safe;


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

    private DpeqConT m_con;
    private MonoTime m_lastRelease;
    private ConnectionState m_state;

    @property ConnectionState state() const { return m_state; }

    package void markReleaseTime()
    {
        m_lastRelease = MonoTime.currTime();
    }

    package @property Duration timeSinceLastRelease() const
    {
        return MonoTime.currTime() - m_lastRelease;
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
        logInfo("Closing connection to %s", m_settings.backendParam.host);
        if (m_state != ConnectionState.closed)
        {
            // notify reader task that it's time to die
            m_state = ConnectionState.closed;
            m_resultQueue.pushBack(null);
        }
        m_con.terminate();
    }

    private void establishConnection()
    {
        assert(m_state == ConnectionState.uninitialized);
        m_state = ConnectionState.connecting;
        scope(failure)
        {
            logError("Failed to construct connection to %s", m_settings.backendParam.host);
            m_state = ConnectionState.closed;
            if (m_con)
            {
                m_con.terminate();
                m_con = null;
            }
        }
        logDebug("Connecting to %s...", m_settings.backendParam.host);
        m_con = new DpeqConT(m_settings.backendParam, m_settings.connectionTimeout);
        logInfo("Connected to %s", m_settings.backendParam.host);
        initializeConnection();
        // start reader task
        m_readerTask = runTask(&readerTaskProc);
        m_lastRelease = MonoTime.currTime();
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
            logDebugV("running conInitQuery: %s", sql);
            m_con.putQueryMessage(sql);
            m_con.flush();
            m_con.pollMessages(null);
        }
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
            PgFuture future = m_resultQueue.popFront(); // blocks
            try
            {
                if (future !is null)
                    future.complete(getQueryResults(m_con));
                else
                    m_con.pollMessages(null);
            }
            catch (PsqlSocketException ex)
            {
                // Socket has thrown, most probably we are dealing with
                // closed connection. We need to flush resultQueue and
                // report this error to everyone.
                logInfo("Connection to %s is assumed closed",
                    m_settings.backendParam.host);
                m_state = ConnectionState.closed;
                m_con.terminate();
                if (future !is null)
                    future.complete(ex);
                // windup remaining waiters
                while (m_resultQueue.length > 0)
                {
                    future = m_resultQueue.popFront();
                    if (future !is null)
                        future.complete(ex);
                }
                // terminate readerTask
                return;
            }
            catch (Exception ex)
            {
                logDebugV("%s caught in readerTask: %s", ex.classinfo.name, ex.msg);
                if (future !is null)
                    future.complete(ex);
                else
                    logWarn("ignoring exception, no accepting future");
            }
        }
    }

    private int m_tsacsBlocked = 0;
    // number of transactions currently blocked on m_tsacMutex or holding it
    package @property int tsacsBlocked() const { return m_tsacsBlocked; }

    // returns true if explicit BEGIN was sent to backend
    private bool beginTsac(in TsacConfig tc, bool allowImplicit)
    {
        logDebugV("begin transaction");
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
        bool allowImplicit = false)
    {
        assert(tc.readonly || !m_settings.readonly,
            "write transaction on read-only connection");
        m_tsacsBlocked++;
        m_tsacMutex.lock();
        scope (exit)
        {
            m_tsacsBlocked--;
            assert(m_tsacsBlocked >= 0);
            m_tsacMutex.unlock();
        }
        if (m_state != ConnectionState.active)
            throw new PsqlSocketException("connection is closed");  // FIXME
        bool explicitTsac = beginTsac(tc, allowImplicit);
        try
        {
            tsacBody(this);
            if (!explicitTsac)
            {
                logDebugV("implicit commit");
                return g_successFuture;
            }
            logDebugV("explicit commit");
            m_con.putQueryMessage("COMMIT");
            m_con.flush();
            PgFuture commitFuture = new PgFuture();
            m_resultQueue.pushBack(commitFuture);
            return commitFuture;
        }
        catch (Exception e)
        {
            if (!explicitTsac)
            {
                logDiagnostic("%s rethrown from implicit rollback: %s",
                    e.classinfo.name, e.msg);
                throw e;
            }
            PgFuture failFuture = new PgFuture();
            failFuture.complete(e);
            logDiagnostic("%s caught, explicit rollback: %s", e.classinfo.name, e.msg);
            m_con.putQueryMessage("ROLLBACK");
            m_con.flush();
            m_resultQueue.pushBack(null);   // rollback result is uninteresting
            return failFuture;
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
    PgFuture execute(scope AbstractPrepared p, bool describe = true, PgFuture f = null)
    {
        if (f is null)
            f = new PgFuture();
        if (p.named)
        {
            string* psName = p.hash in m_psCache;
            if (psName is null)
            {
                logDebugV("Prepared statement cache miss");
                string newName = m_con.getNewPreparedName();
                p.parse(m_con, newName);
                // we need to wait for parse, because it may fail
                m_con.sync();
                m_con.flush();
                auto parseFuture = new PgFuture();
                m_resultQueue.pushBack(parseFuture);
                if (parseFuture.err)
                    throw parseFuture.err;
                m_psCache[p.hash] = newName;
                p.bind(m_con, newName, "");
            }
            else
            {
                logDebugV("Prepared statement cache hit");
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
        m_resultQueue.pushBack(f);
        return f;
    }
}