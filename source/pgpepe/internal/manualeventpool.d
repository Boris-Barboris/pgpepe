module pgpepe.internal.manualeventpool;

import vibe.core.sync: LocalManualEvent, createManualEvent;


@safe:

private
{
    // thread-local stack
    LocalManualEvent*[] eventPool;
    // index of the first free Event object in the pool
    size_t poolHead = 0;

    enum POOL_BATCH = 128;
}

static this()
{
    eventPool.length = POOL_BATCH;
}

LocalManualEvent* getManualEvent()
{
    if (poolHead >= eventPool.length)
        eventPool.length += POOL_BATCH;
    if (eventPool[poolHead] is null)
    {
        eventPool[poolHead] = new LocalManualEvent();
        *(eventPool[poolHead]) = createManualEvent();
    }
    return eventPool[poolHead++];
}

void releaseManualEvent(LocalManualEvent* evt) nothrow
{
    assert(evt !is null);
    assert(poolHead > 0, "pool is already full");
    eventPool[--poolHead] = evt;
    // check if we need to shrink
    if (eventPool.length > poolHead + POOL_BATCH)
    {
        size_t newLength = eventPool.length - POOL_BATCH;
        for (size_t i = eventPool.length - 1; i--; i >= newLength)
            destroy(eventPool[i]);
        eventPool.length = newLength;
    }
}