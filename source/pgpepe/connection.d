module pgpepe.connection;

import core.time;

import vibe.core.net: TCPConnection, connectTCP;
import vibe.core.core: runTask;
import vibe.core.task: Task;
import vibe.core.stream: IOMode;
import vibe.core.sync: TaskMutex;

import dpeq;

import pgpepe.constants;
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
    size_t queueCapacity;
}

enum ConnectionState: byte
{
    uninitialized,
    connecting,
    active,
    closed
}


package alias PgConnDlg = void delegate(scope PgConnection) @safe;


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

    private alias DpeqConT = PSQLConnection!VibeCoreSocket;

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
        establishConnection();
    }

    package void close() nothrow
    {
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
        m_state = ConnectionState.connecting;
        scope(failure)
        {
            if (m_con)
            {
                m_con.terminate();
                m_con = null;
            }
            m_state = ConnectionState.closed;
        }
        m_con = new DpeqConT(m_settings.backendParam, m_settings.connectionTimeout);
        initializeConnection();
        // start reader task
        m_readerTask = runTask(&readerTaskProc);
        m_lastRelease = MonoTime.currTime();
        // we are ready to accept transactions
        m_state = ConnectionState.active;
    }

    private void initializeConnection()
    {
        // initial queries and other stuff
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
                m_state = ConnectionState.closed;
                m_con.terminate();
                if (future !is null)
                    future.complete(ex);
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
                if (future !is null)
                    future.complete(ex);
            }
        }
    }

    private int m_tsacsBlocked = 0;
    // number of transactions currently blocked on m_tsacMutex or holding it
    package @property int tsacsBlocked() const { return m_tsacsBlocked; }

    // returns future to commit result
    package PgFuture runInTsac(ref in TsacConfig tconfig, scope PgConnDlg tsacBody)
    {
        // TODO: fix the code here, this is where retries live
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
        m_con.putQueryMessage("BEGIN;");
        m_con.flush();
        m_resultQueue.pushBack(null);   // we don't care about begin's result
        try
        {
            tsacBody(this);
            m_con.putQueryMessage("COMMIT;");
            m_con.flush();
            PgFuture commitFuture = new PgFuture();
            m_resultQueue.pushBack(commitFuture);
            return commitFuture;
        }
        catch (Exception e)
        {
            m_con.putQueryMessage("ROLLBACK;");
            m_con.flush();
            PgFuture failFuture = new PgFuture();
            failFuture.complete(e);
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
}