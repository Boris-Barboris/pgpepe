module pgpepe.internal.pool;

import core.time: Duration, minutes;
import std.algorithm;

import vibe.core.log;
import vibe.core.sync: LocalTaskSemaphore;

import pgpepe.constants;
import pgpepe.connection;


@safe:



/// Pool of connections to one backend
final class PgConnectionPool
{
    private uint m_fastPoolSize;
    private uint m_slowPoolSize;
    private immutable ConnectionSettings m_settings;

    @property ref immutable(ConnectionSettings) settings() const { return m_settings; }

    this(immutable ConnectionSettings settings, uint fastPoolSize, uint slowPoolSize)
    {
        m_settings = settings;
        m_fastPoolSize = fastPoolSize;
        m_slowPoolSize = slowPoolSize;
        m_fastCons.reserve(fastPoolSize);
        m_slowCons.reserve(slowPoolSize);
        m_fastSema = new LocalTaskSemaphore(m_fastPoolSize * m_settings.queueCapacity);
        m_slowSema = new LocalTaskSemaphore(m_slowPoolSize);
    }

    private PgConnection[] m_fastCons;
    private PgConnection[] m_slowCons;
    private LocalTaskSemaphore m_fastSema;
    private LocalTaskSemaphore m_slowSema;

    /// Schedule transaction on one connection. Blocks on internal semaphore.
    /// Call release after you're done.
    PgConnection lock(bool fast)
    {
        if (fast)
        {
            if (m_fastSema.available == 0)
                logDebug("Blocking on m_fastSema of a connection pool");
            m_fastSema.lock();
            scope(failure) m_fastSema.unlock();
            return chooseFromArray(m_fastCons, m_fastPoolSize, 9);
        }
        else
        {
            if (m_slowSema.available == 0)
                logDebug("Blocking on m_slowSema of a connection pool");
            m_slowSema.lock();
            scope(failure) m_slowSema.unlock();
            return chooseFromArray(m_slowCons, m_slowPoolSize, 2);
        }
    }

    void unlock(bool fast)
    {
        if (fast)
            m_fastSema.unlock();
        else
            m_slowSema.unlock();
    }

    private PgConnection chooseFromArray(ref PgConnection[] pool,
        size_t poolSize, size_t freeCriteria)
    {
        int minLoadIdx = -1;
        ulong minConLoad = ulong.max;
        for (int i = 0; i < poolSize; i++)
        {
            if (i >= pool.length)
            {
                // new connection must be created
                pool ~= new PgConnection(m_settings);
                // start connecting
                pool[i].open();
                return pool[i];
            }
            PgConnection con = pool[i];
            ulong conLoad = con.queueLength + con.tsacsBlocked;
            logTrace("connection #%d load: resultQueue %d + tsacQueue %d",
                i, con.queueLength, con.tsacsBlocked);
            assert(con.state != ConnectionState.uninitialized);
            if (con.state == ConnectionState.connecting && conLoad < freeCriteria)
                return con;
            if (con.state == ConnectionState.closed)
            {
                // connection must be reopened
                pool[i] = con = new PgConnection(m_settings);
                con.open();
                return con;
            }
            if (con.state == ConnectionState.active)
            {
                if (con.timeSinceLastRelease > minutes(30))
                {
                    // connection is too old, postgres probably has closed it
                    con.close();
                    pool[i] = con = new PgConnection(m_settings);
                    con.open();
                    return con;
                }
                if (conLoad < freeCriteria)
                {
                    con.markReleaseTime();
                    return con;
                }
                if (conLoad < minConLoad)
                {
                    minConLoad = conLoad;
                    minLoadIdx = i;
                }
            }
        }
        // at this point we have checked all closed, active and nonexisting
        // connections slots
        if (minLoadIdx >= 0)
        {
            PgConnection con = pool[minLoadIdx];
            con.markReleaseTime();
            return con;
        }
        else
        {
            // no active connections, we need to choose best non-active one
            minLoadIdx = -1;
            minConLoad = ulong.max;
            for (int i = 0; i < poolSize; i++)
            {
                PgConnection con = pool[i];
                assert(con.state != ConnectionState.active);
                assert(con.state != ConnectionState.closed);
                ulong conLoad = con.queueLength + con.tsacsBlocked;
                if (conLoad < minConLoad)
                {
                    minConLoad = conLoad;
                    minLoadIdx = i;
                }
            }
            return pool[minLoadIdx];
        }
    }
}
