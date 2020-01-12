module pgpepe.connection;

import core.time: Duration, MonoTimeImpl, ClockType, seconds;

import vibe.core.net: TCPConnection, connectTCP;
import vibe.core.core: runTask;
import vibe.core.task: Task;
import vibe.core.stream;
import vibe.core.log;
import vibe.core.sync: TaskMutex;
import vibe.stream.tls;

import dpeq;
public import dpeq.authentication;
public import dpeq.connection: SSLPolicy;

import pgpepe.constants;
import pgpepe.exceptions;
import pgpepe.prepared;
import pgpepe.future;
import pgpepe.result;
import pgpepe.internal.taskqueue;



@safe:


struct ConnectionSettings
{
    /// Hostname or IP address of backend.
    string host;
    /// TCP port number.
    ushort port;
    /// User to use to connect to db
    string user;
    /// Database name
    string databaseName;
    /// Authentication plugin. For example, dpeq.authentication.PasswordAuthenticator.
    IPSQLAuthenticator authenticator;
    /// Default timeouts that are applied to socket or events during connection initialization.
    TimeoutConfig timeouts;
    /// TLS configuration.
    TLSConfig tls;
    /// Default transaction type to set after authentication.
    TsacConfig defaultTsacConfig;
    /// Array of simple SQL queries to run in the end of connection initialization,
    /// but after 'defaultTsacConfig' application.
    string[] conInitQueries;
    /// Command queue capacity. Equals to the maximum number of outgoing requests, pushed to the wire.
    uint queueCapacity;
}


/// TCP socket operation timeouts.
struct TimeoutConfig
{
    Duration connectTimeout = seconds(10);
    Duration readTimeout = seconds(10);
    // Duration writeTimeout = seconds(10); not yet implemented in vibe-core
}


/// SSL policy and config
struct TLSConfig
{
    /// SSL policy for dpeq connection.
    SSLPolicy policy = SSLPolicy.PREFER;
    /// Certificate validation mode. Set to TLSPeerValidationMode.none
    /// to disable validation.
    TLSPeerValidationMode validationMode = TLSPeerValidationMode.trustedCert;
    /// File that contains the list of trusted certificate authorities.
    string trustedCertFile = "/etc/ssl/certs/ca-certificates.crt";
}


/// States of PgVibedConnection
enum PgVibedConnectionState: byte
{
    /// Connection is just constructed.
    NEW,
    /// open() was called on the connection and it is currently initializing
    CONNECTING,
    /// open() has succeeded, transport is open, session is initialized and alive.
    ACTIVE,
    /// close() was called.
    CLOSED
}


/** Transaction body delegate signature. Run queries while exclusively owning
connection object and return (commit is issued by pgpepe on return, or
rollback if something was thrown). */
alias TsacDlg = void delegate(scope PgVibedConnection) @trusted;


/** Stream, connected to PSQL backend, and transaction queue. */
class PgVibedConnection
{
    private ConnectionSettings m_settings;

    /// Vibe-d transport implementation that supports TLS and vibe-core eventloop.
    private class VibedTransport: ITransport
    {
        private
        {
            TCPConnection m_con;
            TLSStream m_tlsStream;
            /// Either wrapped TCPConnection or tls stream
            Stream m_stream;
        }

        void close() nothrow
        {
            // we do not finalize TLS, we want immediate socket termination.
            m_con.close();
        }

        /// Try to open TCP connection.
        void connect()
        {
            m_con = connectTCP(m_settings.host, m_settings.port, null, 0,
                m_settings.timeouts.connectTimeout);
            m_con.readTimeout = m_settings.timeouts.readTimeout;
            m_con.keepAlive = true;
            // m_con.writeTimeout = m_settings.timeouts.writeTimeout;
            m_stream = StreamProxy(m_con);
        }

        @property bool supportsSSL() nothrow
        {
            return true;
        }

        void performSSLHandshake()
        {
            auto sslctx = createTLSContext(TLSContextKind.client);
            sslctx.peerValidationMode = m_settings.tls.validationMode;
            if (m_settings.tls.validationMode & TLSPeerValidationMode.checkTrust)
                sslctx.useTrustedCertificateFile(m_settings.tls.trustedCertFile);
            m_stream = createTLSStream(m_con, sslctx);
        }

        void send(const(ubyte)[] buf)
        {
            try
            {
                m_stream.write(buf);
            }
            catch (Exception ex)
            {
                throw new TransportException("Error on stream write", ex);
            }
        }

        void receive(ubyte[] dest)
        {
            try
            {
                m_stream.read(buf);
            }
            catch (Exception ex)
            {
                throw new TransportException("Error on stream read", ex);
            }
        }

        private @property readTimeout(Duration rhs)
        {
            m_con.readTimeout = rhs;
        }
    }

    package alias DpeqConT = PSQLConnection;
    private alias CoarseMonoTime = MonoTimeImpl!(ClockType.coarse);

    // handle to underlying Dpeq connection
    private DpeqConT m_con;
    private VibedTransport m_transport;

    private CoarseMonoTime m_lastRelease;
    private PgConnectionState m_state;

    /// Current connection state.
    @property PgConnectionState state() const nothrow { return m_state; }

    package void markReleaseTime()
    {
        m_lastRelease = CoarseMonoTime.currTime();
    }

    /// Time that has passed since the last time the connection was release to pool.
    package @property Duration timeSinceLastRelease() const
    {
        return CoarseMonoTime.currTime() - m_lastRelease;
    }

    package this(ConnectionSettings settings)
    {
        m_settings = settings;
        m_writeMutex = new TaskMutex();
        m_resultQueue = TaskQueue!PgFuture(m_settings.queueCapacity);
    }

    /// Opens and initialized connection.
    package void open()
    {
        assert(m_state == PgConnectionState.NEW);
        m_writeMutex.lock();
        scope (exit) m_writeMutex.unlock();
        if (m_state == PgConnectionState.NEW)
            establishConnection();
    }

    /// Immediately closes the transport and switches state to CLOSED.
    package void close() nothrow
    {
        if (m_state != PgConnectionState.CLOSED)
        {
            m_state = PgConnectionState.CLOSED;
            if (m_con)
            {
                logInfo("Closing connection to %s", m_settings.backendParam.host);
                m_con.close(false);
            }
            // notify the reader task that it's time to die
            m_resultQueue.pushBack(ReaderCommand(ReaderCommandTag.EXIT_LOOP));
        }
    }

    private void establishConnection()
    {
        assert(m_state == PgConnectionState.NEW);
        m_state = PgConnectionState.CONNECTING;
        scope(failure)
        {
            logError("Failed to establish connection to %s", m_settings.host);
            m_state = PgConnectionState.CLOSED;
            if (m_con)
            {
                m_con.close(false);
                m_con = null;
            }
            if (m_transport)
            {
                m_transport.close();
                m_transport = null;
            }
        }
        logDebug("Connecting to %s...", m_settings.host);
        m_transport = new VibedTransport();
        m_transport.connect();
        m_con = new DpeqConT(m_transport, m_settings.tls.policy);
        logInfo("Connected to %s", m_settings.host);
        initializeSession();
        m_lastRelease = CoarseMonoTime.currTime();
        // start reader task
        m_readerTask = runTask(&readerTaskProc);
        // we are now ready to accept transactions
        m_state = PgConnectionState.ACTIVE;
    }

    /// Last ErrorResponse message that was received from backend
    private NoticeOrError m_lastError;

    /// simply poll until ErrorResponse is received
    private PollAction simpleErrorPoller(PSQLConnection con, RawBackendMessage msg)
    {
        if (msg.type == BackendMessageType.ErrorResponse)
        {
            m_lastError = NoticeOrError.parse(msg.data);
            return PollAction.BREAK;
        }
        return PollAction.CONTINUE;
    }

    /// simply poll messages from m_con until RFQ is received. Throw on any error.
    private void pollUntilRfq()
    {
        PollResult res;
        do
        {
            res = m_con.pollMessages(&simpleErrorPoller);
            if (res == PollResult.POLL_CALLBACK_BREAK)
                throw new PSQLErrorResponseException(m_lastError);
        } while (res != PollResult.RFQ_RECEIVED);
    }

    private void initializeSession()
    {
        // set session default transaction mode
        logDebug("setting default transaction mode for %s",
            m_settings.backendParam.host);
        m_con.sendMessage(
            buildQueryMessage(
                "SET SESSION CHARACTERISTICS AS TRANSACTION " ~
                beginTsacStr(TsacConfig(m_settings.defaultTsacConfig)])));
        pollUntilRfq();
        // confirm that it works
        logDebug("validating transaction settings for %s",
                m_settings.backendParam.host);
        m_con.sendMessage(buildQueryMessage("BEGIN"));
        m_con.sendMessage(buildQueryMessage("COMMIT"));
        pollUntilRfq();
        pollUntilRfq();
        // perform all initialization queries
        foreach (sql; m_settings.conInitQueries)
        {
            logDebug("running conInitQuery: %s", sql);
            m_con.sendMessage(buildQueryMessage(sql));
            pollUntilRfq();
        }
        logDebug("Connection initialized");
    }

    /// Thread-local mutex that guards connection's write buffer and only lets in
    /// one fiber/transaction.
    private TaskMutex m_writeMutex;

    private enum ReaderCommandTag
    {
        /// Read messages until RFQ is received.
        READ_TILL_RFQ,
        /// Read messages until RFQ is received and fill PgFuture with data encountered.
        READ_TO_FUTURE,
        /// Reader task is to exit it's processing loop.
        EXIT_LOOP
    }

    private struct ReaderCommand
    {
        ReaderCommandTag tag;
        PgFuture future;
        PgFutureResultUnion.Kind futureResultKind;
        /// When it is a prepared statement, we cache it's description to
        /// reduce chatter. This however requires reader task to know what description
        /// to put into the future's result.
        const(NamedRowDescription)* cachedDescription;
    }

    /// Queue of commands that the reader task will execute.
    private TaskQueue!ReaderCommand m_resultQueue;

    /// Fiber that repeatedly reads from connection and completes futures.
    private Task m_readerTask;

    /// Roughly equals to number of to-be-read futures, queued on the connection.
    @property size_t queueLength() const { return m_resultQueue.length; }

    /// Main reader task loop.
    private void readerTaskProc()
    {
        while (true)
        {
            // blocks fiber on queue
            ReaderCommand command = m_resultQueue.popFront();
            PgFuture future = null;
            if (command.tag == ReaderCommandTag.EXIT_LOOP)
                return;
            try
            {
                switch (command.tag)
                {
                    case (ReaderCommandTag.READ_TILL_RFQ):
                    {
                        PollResult res;
                        do
                        {
                            res = m_con.pollMessages(&simpleErrorPoller);
                        } while (res != PollResult.RFQ_RECEIVED);
                        break;
                    }
                    case (ReaderCommandTag.READ_TO_FUTURE):
                    {
                        future = command.future;
                        assert(future !is null);
                        future.complete(getQueryResults(m_con));
                        break;
                    }
                    default:
                        assert(0);
                }
            }
            catch (TransportException ex)
            {
                /// socket is broken, closing everything
                if (m_state != PgConnectionState.CLOSED)
                {
                    m_state = PgConnectionState.CLOSED;
                    m_con.close(false);
                }
                if (future)
                    future.complete(ex);
                // windup remaining waiters.
                while (m_resultQueue.length > 0)
                {
                    command = m_resultQueue.popFront();
                    if (command.tag == ReaderCommandTag.READ_TO_FUTURE)
                    {
                        future = command.future;
                        assert(future);
                        future.complete(ex);
                    }
                }
                // Absence of new waiters after this point is guaranteed by
                // close() and terminate().
                return;
            }
            catch (Exception ex)
            {
                logDebug("%s caught in readerTask: %s", ex.classinfo.name, ex.msg);
                if (future)
                    future.complete(ex);
            }
        }
    }

    private int m_tsacsBlocked = 0;

    /// number of transactions currently blocked on m_writeMutex or holding it.
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
        // permissions correctly in initializeSession.
        m_resultQueue.pushBack(null);
        return true;
    }

    // returns future to commit result
    package PgFuture runInTsac(in TsacConfig tc, scope TsacDlg tsacBody,
        bool allowImplicit = false) @trusted
    {
        assert(tc.readonly || !m_settings.readonly,
            "write transaction on read-only connection");
        m_tsacsBlocked++;
        if (m_tsacsBlocked > 1)
            logDebug("Blocking on connection m_writeMutex behind %d contenders", m_tsacsBlocked - 1);
        m_writeMutex.lock();
        scope (exit)
        {
            m_tsacsBlocked--;
            assert(m_tsacsBlocked >= 0);
            m_writeMutex.unlock();
        }
        if (m_state != PgConnectionState.ACTIVE)
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