[![Build Status](https://travis-ci.org/Boris-Barboris/pgpepe.svg?branch=master)](https://travis-ci.org/Boris-Barboris/pgpepe)

# pgpepe
Pgpepe is a library for D that should help you tackle mundane tasks of issuing requests
to Postgres cluster from your vibe-d service. It is dependent on vibe-core library and witten specifically for it.

For usage examples have a look at the tests in tests/source/main.d

Some simple examples:
```d
import pgpepe;

PgConnector c;
immutable ConnectorSettings conSets;

shared static this()
{
    conSets.rwBackends = [
        BackendParams("127.0.0.1", ushort(5432), "postgres", "postgres", "pgpepetestdb")];
}

void createConnector()
{
    c = new PgConnector(conSets);
}

void testPreparedStatement1()
{
    static HashedSql hsql = HashedSql("SELECT $1::int - $2");
    auto ps = prepared(hsql, "13", 3);
    int result = c.execute(ps).asType!int;
    writeln(`result: `, result);
    assert(result == 10);
    // cached prepared statement will be used here
    result = c.execute(ps).asType!int;
    assert(result == 10);
}

void testPreparedStatement2()
{
    auto ps = prepared("SELECT '123'::text, 14::real");
    struct ResT
    {
        string txtfield;
        float floatfield;
    }
    ps.resFCodes = fcodesForStruct!ResT;
    ResT result = c.execute(ps).asStruct!ResT;
    assert(result.txtfield == "123");
    assert(result.floatfield == 14.0f);
}

void testTsac1()
{
    try
    {
        c.transaction((scope PgConnection c) {
                PgFuture createF = c.execute("create table haskdhja adk;");
                createF.throwIfErr();
            });
        assert(0, "should have thrown");
    }
    catch (PsqlErrorResponseException pex)
    {
        writeln("caught as expected: ", pex.msg);
    }
    c.execute("create table testt1 (somerow boolean);");
    c.transaction((scope sc) {
        auto lockF = sc.execute("lock table testt1 in ACCESS EXCLUSIVE MODE;");
        lockF.throwIfErr();
        try
        {
            PgConnector c2 = new PgConnector(conSets);
            // same table lock should fail
            c2.transaction((scope sc) {
                auto lockF2 = sc.execute("lock table testt1 in ACCESS EXCLUSIVE MODE NOWAIT;");
                lockF2.throwIfErr();
            });
            assert(0, "should have thrown");
        }
        catch (PsqlErrorResponseException pex)
        {
            writeln("caught as expected: ", pex.msg, ", sqlcode ", pex.notice.code);
        }
    });
}
```

## Cartoons

![high-level overview](https://raw.githubusercontent.com/Boris-Barboris/pgpepe/master/docs/pgpepe-high-level.png "Overview")

![fast transactions](https://raw.githubusercontent.com/Boris-Barboris/pgpepe/master/docs/pgpepe_fast_tsac_wire.png "Fast transactions on the wire")

![transaction state machine](https://raw.githubusercontent.com/Boris-Barboris/pgpepe/master/docs/pgpepe_tsac_sm.png "Transaction state machine")