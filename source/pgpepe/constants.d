module pgpepe.constants;

import std.container.rbtree;


enum IsolationLevel: byte
{
    READ_COMMITTED,
    REPEATABLE_READ,
    SERIALIZABLE
}


/// Transaction attributes that are respected by pgpepe.
struct TsacConfig
{
    /// Requested isolation level.
    IsolationLevel isolation = IsolationLevel.READ_COMMITTED;
    /// READ ONLY clause of BEGIN.
    bool readonly = false;
    /// The DEFERRABLE transaction property has no effect unless the transaction is also SERIALIZABLE and READ ONLY. When all three of these properties are selected for a transaction, the transaction may block when first acquiring its snapshot, after which it is able to run without the normal overhead of a SERIALIZABLE transaction and without any risk of contributing to or being canceled by a serialization failure. This mode is well suited for long-running reports or backups.
    bool deferrable = false;
}


/// shortcut for read committed read-write
immutable TsacConfig TC_DEFAULT = TsacConfig(IsolationLevel.READ_COMMITTED, false, false);
/// shortcut for repeatable read read-write
immutable TsacConfig TC_REPEATABLEREAD = TsacConfig(IsolationLevel.REPEATABLE_READ, false, false);
/// shortcut for serializable read-write
immutable TsacConfig TC_SERIAL = TsacConfig(IsolationLevel.SERIALIZABLE, false, false);


@trusted:


// Following code prepares static mapping from all possible TsacConfig's to SQL string.
private
{
    __gshared string[TsacConfig] g_tsacStrCache;

    string deferrableStr(TsacConfig tc)
    {
        if (tc.deferrable)
            return "DEFERRABLE;";
        else
            return "NOT DEFERRABLE;";
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

    for (byte i = IsolationLevel.min; i <= IsolationLevel.max; i++)
        for (byte ro = 0; ro < 2; ro++)
            for (byte df = 0; df < 2; df++)
            {
                TsacConfig conf = TsacConfig(
                    cast(IsolationLevel) i, cast(bool) ro, cast(bool) df);
                // writeln("cell: ", cell);
                g_tsacStrCache[conf] = isolationLvlStr(conf);
            }
}


@safe:

/// Get BEGIN sql string for transaction config
string beginTsacStr(TsacConfig tc)
{
    return g_tsacStrCache[tc];
}