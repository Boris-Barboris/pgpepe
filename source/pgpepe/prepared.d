module pgpepe.prepared;

import std.array: Appender, appender;
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


abstract class BasePrepared
{
    private string m_sql;
    final @property string sql() pure const { return m_sql; }

    private ulong m_hash = 0;
    final @property long hash() pure const { return m_hash; }

    private bool m_named = false;
    final @property bool named() pure const { return m_named; }

    private immutable(FormatCode)[] m_resFCodes;
    final @property immutable(FormatCode)[] resFCodes() pure const { return m_resFCodes; }

    /// Set column format codes, requested from the backend. If unset, defaults to all text.
    final @property void resFCodes(immutable(FormatCode)[] rhs) pure { m_resFCodes = rhs; }

    protected final @property void sql(string rhs) pure
    {
        m_sql = rhs;
        m_named = false;
    }

    protected final @property void hsql(const HashedSql rhs) pure
    {
        m_sql = rhs.m_sql;
        m_named = true;
        m_hash = rhs.hash;
    }

    private this() {}

    this(string sql)
    {
        debug lookForTsacs(sql);
        assert(sql.length > 0, "empty sql string");
        m_sql = sql;
    }

    this(const HashedSql hsql)
    {
        assert(hsql.sql.length > 0, "empty sql string");
        m_sql = hsql.m_sql;
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

    private
    {
        const ParamTypes m_params;

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
            static if (spec.typeId == PgType.VARCHAR)
                t_oids[i] = 0;
            else
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
        pure @trusted:

            private int idx = 0;
            private this(int i) { idx = i; }
            int opCall(ubyte[] buf) const nothrow
            {
                return g_serializers[idx](buf, paramPtrs[idx]);
            }
        }

        struct MarshRange
        {
        pure @trusted:

            private int idx = 0;
            @property bool empty() const { return idx >= ParamTypes.length; }
            void popFront() { idx++; }
            @property ParamMarsh front() const
            {
                return ParamMarsh(idx);
            }
        }

        // dpeq has shit type support, so we better stay at text format
        con.putBindMessage(portal, ps, g_formatCodes[], MarshRange(), m_resFCodes);
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


final class PreparedBuilder: BasePrepared
{
    private bool built = false;
    private Appender!string sqlAppender;

    /// Construct unnamed (non-cached) prepared statement builder
    this(short estParamCount = 8)
    {
        sqlAppender.reserve(64);
        m_paramTypes.reserve(estParamCount);
        m_fcodes.reserve(estParamCount);
        m_params.reserve(estParamCount);
        m_serializers.reserve(estParamCount);
    }

    void build(bool named = false)
    {
        assert(!built, "prepared statement already built");
        if (named)
            this.hsql = HashedSql(sqlAppender.data);
        else
            this.sql = sqlAppender.data;
        built = true;
    }

    void build(HashedSql hsql)
    {
        assert(!built, "prepared statement already built");
        this.hsql = hsql;
        built = true;
    }

    private OID[] m_paramTypes;
    private FormatCode[] m_fcodes;
    private const(void)*[] m_params;
    private SerializeF[] m_serializers;

    void append(string sqlPart)
    {
        sqlAppender.put(sqlPart);
    }

    void append(char sqlPart)
    {
        sqlAppender.put(sqlPart);
    }

    void put(T)(const(T)* param)
    {
        assert(!built, "prepared statement already built");
        enum FieldSpec fs = specForType!T;
        static if (fs.typeId == PgType.VARCHAR)
            m_paramTypes ~= 0;
        else
            m_paramTypes ~= fs.typeId;
        m_fcodes ~= DefaultSerializer!fs.formatCode;
        m_params ~= param;
        m_serializers ~= DefaultSerializer!fs.serialize;
    }

    override void parse(PgConnection.DpeqConT con, string psname = "") const
    {
        assert(built, "Statement is not built");
        logDebug("Parsing prepared statement %s", m_sql);
        con.putParseMessage(psname, m_sql, m_paramTypes);
    }

    // put bind message
    override void bind(PgConnection.DpeqConT con, string ps, string portal) const @trusted
    {
        assert(built, "Statement is not built");

        // prepare marshallng functors
        static struct ParamMarsh
        {
        pure @trusted:

            private const PreparedBuilder pb;
            private int idx = 0;
            this(const PreparedBuilder pb, int idx)
            {
                this.pb = pb;
                this.idx = idx;
            }
            int opCall(ubyte[] buf) const nothrow
            {
                return pb.m_serializers[idx](buf, pb.m_params[idx]);
            }
        }

        static struct MarshRange
        {
        pure @trusted:

            const PreparedBuilder pb;
            private int idx = 0;
            @property bool empty() const { return idx >= pb.m_params.length; }
            void popFront() { idx++; }
            @property ParamMarsh front() const
            {
                return ParamMarsh(pb, idx);
            }
        }

        // dpeq has shit type support, so we better stay at text format
        con.putBindMessage(portal, ps, m_fcodes, MarshRange(this), m_resFCodes);
    }
}

auto preparedBuilder() @system
{
    return scoped!PreparedBuilder();
}

@system unittest
{
    auto p = preparedBuilder();
    p.append("select ");
    p.append("$1 +");
    p.append("$2;");
    int a = 3;
    p.put(&a);
    int b = 4;
    p.put(&b);
    p.build();
}