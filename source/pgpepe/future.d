module pgpepe.future;

import dpeq.result;
import vibe.core.sync: LocalManualEvent;

import pgpepe.internal.manualeventpool;


@safe:

/// Future wich will contain query result or the exception. Can be completed
/// only once.
final class PgFuture
{
    private bool m_completed = false;
    @property bool completed() const nothrow { return m_completed; }

    private QueryResult m_result;
    private Exception m_err;
    private LocalManualEvent* m_event;

    @property Exception err()
    {
        await();
        return m_err;
    }

    @property void err(Exception rhs)
    {
        assert(m_err is null);
        m_err = rhs;
    }

    /// throws if future resulted in error
    @property QueryResult result()
    {
        await();
        if (m_err !is null)
            throw m_err;
        return m_result;
    }

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

    void complete(QueryResult res) nothrow
    {
        assert(!m_completed);
        m_result = res;
        m_completed = true;
        if (m_event)
            m_event.emit();
    }

    void complete(Exception ex) nothrow
    {
        assert(!m_completed);
        m_err = ex;
        m_completed = true;
        if (m_event)
            m_event.emit();
    }
}