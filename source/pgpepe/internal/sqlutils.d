module pgpepe.internal.sqlutils;

import std.algorithm.comparison: equal;
import std.uni: asUpperCase;


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