module pgpepe.internal.manualeventpool;

import vibe.core.sync: LocalManualEvent, createManualEvent;


@safe:

// TLS
private
{
    // stack, essentially
    LocalManualEvent*[] g_eventPool;
    size_t poolHead = 0;
}

static this()
{
    g_eventPool.length = 128;
}

LocalManualEvent* getManualEvent()
{
    if (poolHead >= g_eventPool.length)
        g_eventPool.length += 128;
    if (g_eventPool[poolHead] is null)
    {
        g_eventPool[poolHead] = new LocalManualEvent();
        *g_eventPool[poolHead] = createManualEvent();
    }
    return g_eventPool[poolHead++];
}

void releaseManualEvent(LocalManualEvent* evt) nothrow
{
    assert(poolHead > 0);
    assert(evt !is null);
    g_eventPool[poolHead--] = evt;
}