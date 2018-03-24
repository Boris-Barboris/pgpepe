module pgpepe.internal.pool;

import core.time: Duration, minutes;
import std.algorithm;

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
    }

    private PgConnection[] m_fastCons;
    private PgConnection[] m_slowCons;

    /// schedule transaction on one connection
    PgConnection getConnection(bool fast)
    {
        if (fast)
            return chooseFromArray(m_fastCons, m_fastPoolSize, 9);
        else
            return chooseFromArray(m_slowCons, m_slowPoolSize, 1);
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