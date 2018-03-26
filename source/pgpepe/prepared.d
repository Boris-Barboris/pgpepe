module pgpepe.prepared;

import std.container.array;
import std.range: iota;
public import std.typecons: Nullable;

import dpeq;

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

/// Prepared statement with variadic parameter syntax
struct Prepared(ParamTypes...)
    if (ParamTypes.length > 0)
{
    package string m_sql;
    @property string sql() const { return m_sql; }
    package ulong hash = 0;
    package bool named = false;

    /// Construct unnamed (non-cached) prepared statement
    this(ConsTypes...)(string sql, ConsTypes params)
        if (ConsTypes.length == ParamTypes.length)
    {
        debug lookForTsacs(sql);
        assert(sql.length > 0, "empty sql string");
        m_sql = sql;
        m_params = params;
    }

    /// Construct named (cached) prepared statement
    this(ConsTypes...)(const HashedSql hsql, ConsTypes params)
        if (ConsTypes.length == ParamTypes.length)
    {
        assert(hsql.sql.length > 0, "empty sql string");
        m_sql = hsql.sql;
        named = true;
        hash = hsql.hash;
        m_params = params;
    }

    private
    {
        ParamTypes m_params;

        // static type-specific data
        static immutable(OID)[] g_paramTypes;
        static immutable(FormatCode)[] g_paramFcodes;
    }

    static this()
    {
        static foreach (i; iota(0, ParamTypes.length))
        {{
            enum OID oid = oidForType!(ParamTypes[i]);
            g_paramTypes ~= oid;
            g_paramFcodes ~= StaticFieldSerializer!(FieldSpec(oid, true)).formatCode;
        }}
    }
}

unittest
{
    auto p = Prepared!(double, Nullable!string)("asd", 13.0, "32232");
    assert(p.m_params[0] == 13.0);
    assert(p.m_params[1].get == "32232");
    p = typeof(p)("asd2", 12.0, Nullable!string());
    assert(p.m_params[1].isNull);
}