// Some tests and shit

import std.exception: assertThrown;
import std.stdio;

import vibe.core.task: Task;
import vibe.core.core: runEventLoop, runTask, exitEventLoop;
import vibe.core.log;

import dpeq;
import pgpepe;


PgConnector c;
immutable ConnectorSettings conSets;

shared static this()
{
    conSets.rwBackends = [
        BackendParams("127.0.0.1", ushort(5432), "postgres", "r00tme", "pgpepetestdb")];
}

void main()
{
    setLogLevel(LogLevel.debugV);
    setLogFormat(FileLogger.Format.threadTime, FileLogger.Format.threadTime);
    Task t = runTask(&runTestList);
    runEventLoop();
    writeln("all tests passed");
}

void runTestList()
{
    scope (exit) exitEventLoop();
    c = new PgConnector(conSets);
    testSimpleQuery1();
    testExceptionSimple();
    testPreparedStatement1();
    testPreparedStatement2();
    testPreparedStatement3();
    testTsac1();
}

void testSimpleQuery1()
{
    writeln(__FUNCTION__);
    QueryResult r = c.execute("SELECT version();");
    assert(r.blocks.length == 1);
    auto variants = blockToVariants(r.blocks[0]);
    assert(variants.length == 1);
    writeln(`"SELECT version();" returned: `, variants[0].front);
    string expectedVersion = variants[0].front.get!string;
    Task[] tasks;
    immutable size_t tcount = 8;
    tasks.reserve(tcount);
    for (size_t i = 0; i < tcount; i++)
        tasks ~= runTask(() { assertVersion(c, expectedVersion); });
    for (size_t i = 0; i < tcount; i++)
        tasks[i].join();
}

void assertVersion(PgConnector c, string expected)
{
    QueryResult r = c.execute("SELECT version();");
    assert(r.blocks.length == 1);
    auto variants = blockToVariants(r.blocks[0]);
    assert(variants.length == 1);
    assert(variants[0].front.get!string == expected);
}

void testExceptionSimple()
{
    assertThrown!PsqlErrorResponseException(c.execute("SELECT bullshit();"));
}

void testPreparedStatement1()
{
    writeln(__FUNCTION__);
    auto ps = prepared("SELECT $1 + $2", 12, 3);
    QueryResult r = c.execute(ps);
    auto variants = blockToVariants(r.blocks[0]);
    assert(variants.length == 1);
    int result = variants[0].front.get!int;
    writeln(`result: `, result);
    assert(result == 15);
}

void testPreparedStatement2()
{
    writeln(__FUNCTION__);
    static HashedSql hsql = HashedSql("SELECT $1::int - $2");
    auto ps = prepared(hsql, "13", 3);
    QueryResult r = c.execute(ps);
    auto variants = blockToVariants(r.blocks[0]);
    assert(variants.length == 1);
    int result = variants[0].front.get!int;
    writeln(`result: `, result);
    assert(result == 10);
    // cached prepared statement should be used here
    r = c.execute(ps);
    variants = blockToVariants(r.blocks[0]);
    result = variants[0].front.get!int;
    assert(result == 10);
}

void testPreparedStatement3()
{
    writeln(__FUNCTION__);
    auto ps = prepared("SELECT '123'");
    QueryResult r = c.execute(ps);
    auto variants = blockToVariants(r.blocks[0]);
    assert(variants.length == 1);
    string result = variants[0].front.get!string;
    writeln(`result: `, result);
    assert(result == "123");
}

void testTsac1()
{
    writeln(__FUNCTION__);
    try
    {
        // should throw
        c.transaction((scope c) {
                auto createF = c.execute("create table haskdhja adk;");
                createF.result();
            });
    }
    catch (PsqlErrorResponseException pex)
    {
        writeln("caught as expected: ", pex.msg);
    }
    c.execute("create table testt1 (somerow boolean);");
    c.transaction((scope sc) {
        auto lockF = sc.execute("lock table testt1 in ACCESS EXCLUSIVE MODE;");
        lockF.result();
        // another transaction with the same lock
        try
        {
            PgConnector c2 = new PgConnector(conSets);
            c2.transaction((scope sc) {
                auto lockF2 = sc.execute("lock table testt1 in ACCESS EXCLUSIVE MODE NOWAIT;");
                assert(lockF2.err !is null);
                throw lockF2.err;
            });
            assert(0, "should have thrown");
        }
        catch (PsqlErrorResponseException pex)
        {
            writeln("caught as expected: ", pex.msg, ", sqlcode ", pex.notice.code);
        }
    });
}