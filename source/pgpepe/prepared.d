module pgpepe.prepared;

import std.container.array;
import std.range: iota;
public import std.typecons: Nullable;
import std.typecons: scoped;

import vibe.core.log;

import dpeq;

import pgpepe.connection: PgConnection;
import pgpepe.internal.sqlutils;
import pgpepe.internal.typemap;


@safe:


/// Pair of SQL statement and it's hash.
/// Recommended to be computed at compile-time.
struct HashedSql
{
    package string m_sql;
    @property string sql() const { return m_sql; }
    package ulong hash;
    this(string sql)
    {
        debug lookForTsacs(sql);
        this.m_sql = sql;
        hash = djb2(sql);
    }
}


class BasePrepared
{
    private string m_sql;
    final @property string sql() pure const { return m_sql; }

    private ulong m_hash = 0;
    final @property long hash() pure const { return m_hash; }

    private bool m_named = false;
    final @property bool named() pure const { return m_named; }

    this(string sql)
    {
        debug lookForTsacs(sql);
        assert(sql.length > 0, "empty sql string");
        m_sql = sql;
    }

    this(const HashedSql hsql)
    {
        assert(hsql.sql.length > 0, "empty sql string");
        m_sql = hsql.sql;
        m_named = true;
        m_hash = hsql.hash;
    }

    void parse(PgConnection.DpeqConT con, string psname = "") const
    {
        logDebug("Parsing prepared statement %s", m_sql);
        con.putParseMessage(psname, m_sql, OID[].init);
    }

    abstract void bind(PgConnection.DpeqConT con, string ps, string portal) const @trusted;
}


/// Prepared statement with variadic parameter syntax
final class Prepared(ParamTypes...): BasePrepared
{
    /// Construct unnamed (non-cached) prepared statement
    this(string sql, ParamTypes params)
    {
        super(sql);
        m_params = params;
    }

    /// Construct named (cached) prepared statement
    this(const HashedSql hsql, ParamTypes params)
    {
        super(hsql);
        m_params = params;
    }

    static if (ParamTypes.length > 0)
    {
        /// Construct named (cached) prepared statement
        this(const HashedSql hsql)
        {
            super(hsql);
        }
    }

    /// override params
    final void setParams(ParamTypes params)
    {
        m_params = params;
    }

    private
    {
        ParamTypes m_params;

        // static type-specific data
        static immutable OID[ParamTypes.length] g_paramOids;
        static immutable FormatCode[ParamTypes.length] g_formatCodes;
        static immutable SerializeF[ParamTypes.length] g_serializers;
    }

    shared static this() @trusted
    {
        OID[ParamTypes.length] t_oids;
        FormatCode[ParamTypes.length] t_fcodes;
        SerializeF[ParamTypes.length] t_serial;
        static foreach (i; iota(0, ParamTypes.length))
        {{
            enum FieldSpec spec = specForType!(ParamTypes[i]);
            t_oids[i] = spec.typeId;
            t_fcodes[i] = DefaultSerializer!spec.formatCode;
            t_serial[i] = cast(SerializeF) DefaultSerializer!spec.serialize;
        }}
        g_paramOids = t_oids;
        g_formatCodes = t_fcodes;
        g_serializers = t_serial;
    }

    override void parse(PgConnection.DpeqConT con, string psname = "") const
    {
        logDebug("Parsing prepared statement %s", m_sql);
        con.putParseMessage(psname, m_sql, g_paramOids[]);
    }

    // put bind message
    override void bind(PgConnection.DpeqConT con, string ps, string portal) const @trusted
    {
        // pointers to parameters
        const(void)*[ParamTypes.length] paramPtrs;
        foreach (i, ref param; m_params)
            paramPtrs[i] = &param;

        // prepare marshallng functors
        struct ParamMarsh
        {
            private int idx = 0;
            private this(int i) { idx = i; }
            int opCall(ubyte[] buf) const
            {
                return g_serializers[idx](buf, paramPtrs[idx]);
            }
        }

        struct MarshRange
        {
            private int idx = 0;
            @property bool empty() const { return idx >= ParamTypes.length; }
            void popFront() { idx++; }
            @property ParamMarsh front() const
            {
                return ParamMarsh(idx);
            }
        }

        // dpeq has shit type support, so we better stay at text format
        con.putBindMessage(portal, ps, g_formatCodes[], MarshRange(), [FormatCode.Text]);
    }
}

auto prepared(T...)(string sql, T args) @system
{
    return scoped!(Prepared!T)(sql, args);
}

auto prepared(T...)(HashedSql hsql, T args) @system
{
    return scoped!(Prepared!T)(hsql, args);
}

@system unittest
{
    auto p = prepared("SELECT;", 13.0, Nullable!string("32232"));
    assert(p.m_params[0] == 13.0);
    assert(p.m_params[1].get == "32232");
    p = prepared("SELECT;", 12.0, Nullable!string());
    assert(p.m_params[1].isNull);
}