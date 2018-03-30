module pgpepe.constants;

import std.container.rbtree;

public import dpeq.constants: OID, PgType;


enum IsolationLevel: byte
{
    READ_COMMITTED = 0,
    REPEATABLE_READ,
    SERIALIZABLE
}

immutable IsolationLevel READ_COMMITTED = IsolationLevel.READ_COMMITTED;
immutable IsolationLevel REPEATABLE_READ = IsolationLevel.REPEATABLE_READ;
immutable IsolationLevel SERIALIZABLE = IsolationLevel.SERIALIZABLE;


/// All possible types of transactions are described using this structure.
struct TsacConfig
{
    IsolationLevel isolation = READ_COMMITTED;
    bool readonly = false;
    /**  Transaction should be marked as fast if it:
        1. Does not require round-trips, e.g. subsequent queries do not depend
            on the result of previous ones and are issued in batched manner.
        2. Time, required to process this transaction by postgres is around
            millisecond or less.  */
    bool fast = true;
    /// useful for serializable readonly, consult Psql docks
    bool deferrable = false;
}

static assert (TsacConfig.sizeof == 4);

immutable TsacConfig TSAC_DEFAULT = TsacConfig(READ_COMMITTED, false, false, false);
immutable TsacConfig TSAC_FDEFAULT = TsacConfig(READ_COMMITTED, false, true, false);
immutable TsacConfig TSAC_RR = TsacConfig(REPEATABLE_READ, false, false, false);
immutable TsacConfig TSAC_RORR = TsacConfig(REPEATABLE_READ, true, false, false);
immutable TsacConfig TSAC_FRR = TsacConfig(REPEATABLE_READ, false, true, false);
immutable TsacConfig TSAC_SERIAL = TsacConfig(SERIALIZABLE, false, false, false);
immutable TsacConfig TSAC_FSERIAL = TsacConfig(SERIALIZABLE, false, true, false);
immutable TsacConfig TSAC_SDEFER = TsacConfig(SERIALIZABLE, true, false, true);





// Stuff, used by pgpepe itself

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

    string deferrableStr(in TsacConfig tc)
    {
        if (tc.deferrable)
            return "DEFERRABLE";
        else
            return "NOT DEFERRABLE";
    }

    string readonlyStr(in TsacConfig tc)
    {
        if (tc.readonly)
            return "READ ONLY " ~ deferrableStr(tc);
        else
            return "READ WRITE " ~ deferrableStr(tc);
    }

    string isolationLvlStr(in TsacConfig tc)
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