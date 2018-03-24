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
}

void testSimpleQuery1()
{
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