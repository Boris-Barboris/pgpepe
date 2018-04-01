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
    setLogLevel(LogLevel.debug_);
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
    testPreparedStatement4();
    testPreparedStatement5();
    testTsac1();
    testConversion1();
    testParrallel1();
}

void testSimpleQuery1()
{
    writeln(__FUNCTION__);
    QueryResult r = c.execute("SELECT version();");
    string expectedVersion = r.asType!string;
    writeln(`"SELECT version();" returned: `, expectedVersion);
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
    assert(r.asType!string == expected);
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
    int result = r.asType!int;
    writeln(`result: `, result);
    assert(result == 15);
}

void testPreparedStatement2()
{
    writeln(__FUNCTION__);
    static HashedSql hsql = HashedSql("SELECT $1::int - $2");
    auto ps = prepared(hsql, "13", 3);
    QueryResult r = c.execute(ps);
    int result = r.asType!int;
    writeln(`result: `, result);
    assert(result == 10);
    // cached prepared statement should be used here
    r = c.execute(ps);
    result = r.asType!int;
    assert(result == 10);
}

void testPreparedStatement3()
{
    writeln(__FUNCTION__);
    auto ps = prepared("SELECT '123'::int");
    ps.resFCodes = [FormatCode.Binary];
    QueryResult r = c.execute(ps, false);
    int result = r.asType!int(ps.resFCodes);
    writeln(`result: `, result);
    assert(result == 123);
}

void testPreparedStatement4()
{
    writeln(__FUNCTION__);
    auto pb = preparedBuilder();
    pb.append("SELECT $1::varchar");
    int param1 = 42;
    pb.put(&param1);
    pb.build();
    QueryResult r = c.execute(pb);
    string result = r.asType!string;
    writeln(`result: `, result);
    assert(result == "42");
}

void testPreparedStatement5()
{
    writeln(__FUNCTION__);
    auto ps = prepared("SELECT '123'::text, 14::real");
    struct ResT
    {
        string txtfield;
        float floatfield;
    }
    ps.resFCodes = fcodesForStruct!ResT;
    ResT result = c.execute(ps).asStruct!ResT;
    writeln(`result: `, result);
    assert(result.txtfield == "123");
    assert(result.floatfield == 14.0f);
}

void testTsac1()
{
    writeln(__FUNCTION__);
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

void testConversion1()
{
    writeln(__FUNCTION__);
    c.execute("create table testt2 (
        row1 boolean,
        row2 int,
        row3 bigint,
        row4 double precision,
        row5 money,
        row6 timestamp,
        row7 text);");
    c.execute(`insert into testt2 values ('t', 1, 42, 46.0, '12.23', null, 'sometext');`);
    auto r = c.execute(`select * from testt2`);
    assert(r.asTag == 1);

    enum TestEnum: int
    {
        zero = 0,
        one = 1
    }

    static struct ResS
    {
        bool row1;
        TestEnum row2;
        long row3;
        double row4;
        string row5;
        Nullable!string row6;
        string row7;
    }

    ResS res = r.asStruct!ResS;
    assert(res.row1 == true);
    assert(res.row2 == TestEnum.one);
    assert(res.row3 == 42);
    assert(res.row4 == 46.0);
    assert(res.row5 == "$12.23");
    assert(res.row6.isNull);
    assert(res.row7 == "sometext");
}

void testParrallel1()
{
    writeln(__FUNCTION__);

    // two rw backends and two ro backends
    immutable ConnectorSettings schedConSets = immutable ConnectorSettings(
        [BackendParams("127.0.0.1", ushort(5432), "postgres", "r00tme", "pgpepetestdb"),
        BackendParams("127.0.0.1", ushort(5432), "postgres", "r00tme", "pgpepetestdb")],
        [BackendParams("127.0.0.1", ushort(5432), "postgres", "r00tme", "pgpepetestdb"),
        BackendParams("127.0.0.1", ushort(5432), "postgres", "r00tme", "pgpepetestdb")],
        2, 0);

    PgConnector pc = new PgConnector(schedConSets);

    bool[int] rw_pids;
    bool[int] ro_pids;

    void taskFunc(bool ro)
    {
        TsacConfig tc = TSAC_FDEFAULT;
        tc.readonly = ro;
        auto r = pc.execute("SELECT pg_backend_pid()", tc);
        if (ro)
            ro_pids[r.asType!int] = true;
        else
            rw_pids[r.asType!int] = true;
    }

    Task[] tasks = new Task[128];
    for (size_t i = 0; i < 128; i++)
        tasks[i] = runTask(() { taskFunc(false); });
    for (size_t i = 0; i < 128; i++)
        tasks[i].join();
    for (size_t i = 0; i < 128; i++)
        tasks[i] = runTask(() { taskFunc(true); });
    for (size_t i = 0; i < 128; i++)
        tasks[i].join();
    assert(rw_pids.length == 4);
    assert(ro_pids.length == 4);

    import std.algorithm;
    writeln("readonly backend pids: ", ro_pids.keys);
    writeln("read-write backend pids: ", rw_pids.keys);
    assert(setIntersection(sort(rw_pids.keys), sort(ro_pids.keys)).empty);
}