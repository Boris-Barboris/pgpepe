module pgpepe.prepared;

import dpeq;

import pgpepe.internal.sqlutils;


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

/// Prepared statement
struct Prepared
{
    package string m_sql;
    @property string sql() const { return m_sql; }
    package ulong hash = 0;
    package bool named = false;

    /// Construct unnamed (non-cached) prepared statement
    this(string sql)
    {
        debug lookForTsacs(sql);
        assert(sql.length > 0, "empty sql string");
        m_sql = sql;
    }

    /// Construct named (cached) prepared statement
    this(const HashedSql hsql)
    {
        assert(hsql.sql.length > 0, "empty sql string");
        m_sql = hsql.sql;
        named = true;
        hash = hsql.hash;
    }

    private
    {
        short paramCount = 0;
        FormatCode[] fcodes;
        ubyte[] data;
    }
}