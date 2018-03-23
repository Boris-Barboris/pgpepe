// Some tests and shit

import std.stdio;

import vibe.core.task: Task;
import vibe.core.core: runEventLoop, runTask, exitEventLoop;

import dpeq;
import pgpepe;


void main()
{
    Task t = runTask(&runTestList);
    runEventLoop();
}

void runTestList()
{
    scope (exit) exitEventLoop();
    testSimpleQuery1();
}

immutable ConnectorSettings conSets;

shared static this()
{
    conSets.rwBackends = [
        BackendParams("127.0.0.1", ushort(5432), "postgres", "r00tme", "pgpepetestdb")];
}

void testSimpleQuery1()
{
    writeln("Creating connector object");
    PgConnector c = new PgConnector(conSets);
    writeln("Connector created");
    QueryResult r = c.execute("SELECT version();");
    assert(r.blocks.length == 1);
    auto variants = blockToVariants(r.blocks[0]);
    assert(variants.length == 1);
    writeln(`"SELECT version();" returned: `, variants[0].front);
}