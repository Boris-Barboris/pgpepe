module pgpepe.future;

import dpeq.result;
import vibe.core.log;
import vibe.core.sync: LocalManualEvent;

import pgpepe.internal.manualeventpool;


@safe:


/// Future wich will eventually contain query result or the exception.
final class PgFuture
{
    private bool m_completed = false;
    @property bool completed() const nothrow { return m_completed; }

    private QueryResult m_result;
    private Exception m_err;
    private LocalManualEvent* m_event;

    /// Blocks fiber until completed and returns error exception object or
    /// null if future succeeded.
    @property Exception err()
    {
        await();
        return m_err;
    }

    /// Blocks fiber until completed and returns query result, or throws if
    /// future was completed with an error.
    @property QueryResult result()
    {
        await();
        if (m_err !is null)
            throw m_err;
        return m_result;
    }

    /// Blocks fiber until completed
    void await()
    {
        if (m_completed)
            return;
        assert(m_event is null, "multiple waiters forbidden");
        m_event = getManualEvent();
        m_event.waitUninterruptible();
        assert(m_completed);
        releaseManualEvent(m_event);
        m_event = null;
    }

    /// Succesfully complete the future, assigning result value and signaling
    /// the waiter fiber (if present).
    void complete(QueryResult res) nothrow
    {
        assert(!m_completed, "future already completed");
        m_result = res;
        m_completed = true;
        if (m_event !is null)
            m_event.emitSingle();
    }

    /// Complete the future with exception, signaling
    /// the waiter fiber (if present).
    void complete(Exception ex) nothrow
    {
        assert(!m_completed, "future already completed");
        m_err = ex;
        m_completed = true;
        if (m_event !is null)
            m_event.emitSingle();
    }
}