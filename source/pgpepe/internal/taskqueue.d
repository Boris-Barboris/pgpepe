module pgpepe.internal.taskqueue;

import std.exception: enforce;

import vibe.core.sync: LocalManualEvent, createManualEvent;


@safe:


/// Fixed-size task-blocking queue.
struct TaskQueue(T)
{
    private
    {
        T[] arr;
        size_t ifront = 0;
        size_t len = 0;
        LocalManualEvent pushEvt;
        LocalManualEvent popEvt;
    }

    /// number of elements in queue
    @property size_t length() const { return len; }
    @property size_t capacity() const { return arr.length; }

    this(size_t capacity)
    {
        enforce(capacity != 0, "invalid (zero) capacity");
        arr.length = capacity;
        pushEvt = createManualEvent();
        popEvt = createManualEvent();
    }

    // blocks if full
    void pushBack(T v) nothrow
    {
        if (len == arr.length)
            popEvt.waitUninterruptible();
        assert(len < arr.length);
        arr[(ifront + len) % arr.length] = v;
        len++;
        if (len == 1)
            pushEvt.emitSingle();
    }

    // blocks if empty
    T popFront() nothrow
    {
        if (len == 0)
            pushEvt.waitUninterruptible();
        assert(len > 0);
        len--;
        T val = arr[ifront];
        ifront = (ifront + 1) % arr.length;
        if (len == arr.length - 1)
            popEvt.emitSingle();
        return val;
    }
}