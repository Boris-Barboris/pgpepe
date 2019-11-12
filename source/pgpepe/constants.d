module pgpepe.constants;

import std.container.rbtree;


enum IsolationLevel: byte
{
    READ_COMMITTED,
    REPEATABLE_READ,
    SERIALIZABLE
}

enum IsolationLevel READ_COMMITTED = IsolationLevel.READ_COMMITTED;
enum IsolationLevel REPEATABLE_READ = IsolationLevel.REPEATABLE_READ;
enum IsolationLevel SERIALIZABLE = IsolationLevel.SERIALIZABLE;


/// Transaction attributes that are respected by pgpepe.
struct TsacConfig
{
    IsolationLevel isolation = READ_COMMITTED;
    bool readonly = false;
    /** Fast transactions are routed to separate connection set in order to minimize latency.
        Transaction should be marked as fast if it:
        1. Does not require round-trips, e.g. subsequent queries do not depend
           on the result of previous ones and are issued in batched manner.
        2. Time, required to process this transaction by postgres is around
           millisecond or less.  */
    bool fast = true;
    /// The DEFERRABLE transaction property has no effect unless the transaction is also SERIALIZABLE and READ ONLY. When all three of these properties are selected for a transaction, the transaction may block when first acquiring its snapshot, after which it is able to run without the normal overhead of a SERIALIZABLE transaction and without any risk of contributing to or being canceled by a serialization failure. This mode is well suited for long-running reports or backups.
    bool deferrable = false;
}

immutable TsacConfig TSAC_DEFAULT = TsacConfig(READ_COMMITTED, false, false, false);
immutable TsacConfig TSAC_FDEFAULT = TsacConfig(READ_COMMITTED, false, true, false);
immutable TsacConfig TSAC_RR = TsacConfig(REPEATABLE_READ, false, false, false);
immutable TsacConfig TSAC_RORR = TsacConfig(REPEATABLE_READ, true, false, false);
immutable TsacConfig TSAC_FRR = TsacConfig(REPEATABLE_READ, false, true, false);
immutable TsacConfig TSAC_SERIAL = TsacConfig(SERIALIZABLE, false, false, false);
immutable TsacConfig TSAC_FSERIAL = TsacConfig(SERIALIZABLE, false, true, false);
immutable TsacConfig TSAC_SDEFER = TsacConfig(SERIALIZABLE, true, false, true);



// Internal pgpepe data

@trusted:


private
{
    struct TsacConfCell
    {
        int num;
        string str;
    }

    alias TsacStrTree = RedBlackTree!(TsacConfCell, "a.num < b.num");
    __gshared TsacStrTree tsacStrTree;

    int numForTc(in TsacConfig tc)
    {
        return tc.isolation * 100 + int(tc.readonly) * 10 + int(tc.deferrable);
    }

    string deferrableStr(TsacConfig tc)
    {
        if (tc.deferrable)
            return "DEFERRABLE";
        else
            return "NOT DEFERRABLE";
    }

    string readonlyStr(TsacConfig tc)
    {
        if (tc.readonly)
            return "READ ONLY " ~ deferrableStr(tc);
        else
            return "READ WRITE " ~ deferrableStr(tc);
    }

    string isolationLvlStr(TsacConfig tc)
    {
        final switch (tc.isolation)
        {
            case (IsolationLevel.READ_COMMITTED):
                return "ISOLATION LEVEL READ COMMITTED " ~ readonlyStr(tc);
            case (IsolationLevel.REPEATABLE_READ):
                return "ISOLATION LEVEL REPEATABLE READ " ~ readonlyStr(tc);
            case (IsolationLevel.SERIALIZABLE):
                return "ISOLATION LEVEL SERIALIZABLE " ~ readonlyStr(tc);
        }
    }
}

shared static this()
{
    // import std.stdio

    tsacStrTree = new TsacStrTree();
    for (byte i = 0; i < 3; i++)
        for (byte ro = 0; ro < 2; ro++)
            for (byte df = 0; df < 2; df++)
            {
                TsacConfig conf = TsacConfig(
                    cast(IsolationLevel) i, cast(bool) ro, true, cast(bool) df);
                TsacConfCell cell = TsacConfCell(numForTc(conf), isolationLvlStr(conf));
                // writeln("cell: ", cell);
                tsacStrTree.insert(cell);
            }
}

/// Get transaction string from transaction conig
package string beginTsacStr(in TsacConfig tc)
{
    TsacConfCell key = TsacConfCell(numForTc(tc));
    return tsacStrTree.equalRange(key).front.str;
}