module pgpepe.internal.sqlutils;

import std.algorithm.comparison: equal;
import std.uni: asUpperCase;

import pgpepe.constants: TsacConfig;


@safe:

/// asserts if sql contains explicit transaction management commands
void lookForTsacs(scope const string sql) pure // @nogc
{
    if (sql.length < 5)
        return;
    if (sql[0..5].asUpperCase.equal("BEGIN"))
        assert(0, "BEGIN detected. Please don't use transaction " ~
            "management commands explicitly with pgpepe");
    if (sql[0..5].asUpperCase.equal("START"))
        assert(0, "START detected. Please don't use transaction " ~
            "management commands explicitly with pgpepe");
    if (sql.length < 6)
        return;
    if (sql[0..6].asUpperCase.equal("COMMIT"))
        assert(0, "COMMIT detected. Please don't use transaction " ~
            "management commands explicitly with pgpepe");
}

/// http://www.cse.yorku.ca/~oz/hash.html
ulong djb2(scope const string str) pure
{
	ulong hash = 5381;
	foreach (char c; str)
		hash = ((hash << 5) + hash) + c;
	return hash;
}


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
    for (byte i = IsolationLevel.min; i <= IsolationLevel.max; i++)
        for (byte ro = 0; ro < 2; ro++)
            for (byte df = 0; df < 2; df++)
            {
                TsacConfig conf = TsacConfig(
                    cast(IsolationLevel) i, cast(bool) ro, cast(bool) df);
                g_tsacStrCache[conf] = isolationLvlStr(conf);
            }
}


@safe:

/// Get BEGIN sql string for transaction config
string beginTsacStr(TsacConfig tc)
{
    return g_tsacStrCache[tc];
}