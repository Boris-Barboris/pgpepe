module pgpepe.internal.manualeventpool;

import vibe.core.sync: LocalManualEvent, createManualEvent;


@safe:

private
{
    // thread-local stack
    LocalManualEvent*[] eventPool;
    // index of the first free Event object in the pool
    size_t poolHead = 0;
}

static this()
{
    eventPool.length = 128;
}

LocalManualEvent* getManualEvent()
{
    if (poolHead >= eventPool.length)
        eventPool.length += 128;
    if (eventPool[poolHead] is null)
    {
        eventPool[poolHead] = new LocalManualEvent();
        *eventPool[poolHead] = createManualEvent();
    }
    return eventPool[poolHead++];
}

void releaseManualEvent(LocalManualEvent* evt) nothrow
{
    assert(evt !is null);
    assert(poolHead > 0, "pool is already full");
    eventPool[poolHead--] = evt;
}