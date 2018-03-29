module pgpepe.conv;

import std.algorithm: max;
import std.conv;
import std.exception: enforce;
import std.range: enumerate, isOutputRange;

import dpeq;
public import dpeq.result: QueryResult;

import pgpepe.exceptions;
import pgpepe.internal.meta;
import pgpepe.internal.typemap;
public import pgpepe.internal.typemap: isNullable;


@safe:


alias RIE = ResultInterpretException;

/** Get command tag count from the result. For example, UPDATE returns
command tag "UPDATE rowcount". This function returns rowcount as int. */
int asTag(scope const QueryResult r)
{
    enforce!RIE(r.blocks.length == 1, "Expected one row block");
    const RowBlock b = r.blocks[0];
    enforce!RIE(b.state == RowBlockState.complete, "Row block not in complete state");
    enforce!RIE(b.commandTag.length > 0, "No command tag in the result");
    size_t numStart = b.commandTag.length - 1;
    while (b.commandTag[numStart] != ' ')
        numStart--;
    return b.commandTag[numStart + 1 .. $].to!int;
}

/// Mark fields or properties with this to override their name
struct PgName
{
    string name;
    alias name this;
}


StrT asStruct(StrT)(const QueryResult r, bool strict = true,
    in FormatCode[] resFcodes = null)
{
    enforce!RIE(r.blocks.length == 1, "Expected one row block");
    const RowBlock b = r.blocks[0];
    enforce!RIE(b.state == RowBlockState.complete, "Row block not in complete state");
    enforce!UnexpectedRowCount(b.dataRows.length == 1, "Expected one row");
    StrT res;
    auto mapper = RowMapper!StrT(b.rowDesc, resFcodes);
    mapper.map(b.dataRows[0], res, strict);
    return res;
}

StrT[] asStructs(StrT)(const QueryResult r, bool strict = true,
    in FormatCode[] resFcodes = null)
{
    enforce!RIE(r.blocks.length == 1, "Expected one row block");
    const RowBlock b = r.blocks[0];
    enforce!RIE(b.state != RowBlockState.invalid, "Row block in invalid state");
    StrT[] res;
    if (b.dataRows.length == 0)
        return res;
    res.length = b.dataRows.length;
    auto mapper = RowMapper!StrT(b.rowDesc, resFcodes);
    foreach (i, ref s; res)
        mapper.map(b.dataRows[i], s, strict);
    return res;
}

void asStructs(StrT, ORange)(const QueryResult r, ref ORange or, bool strict = true,
    in FormatCode[] resFcodes = null) if (isOutputRange!(ORange, StrT))
{
    enforce!RIE(r.blocks.length == 1, "Expected one row block");
    const RowBlock b = r.blocks[0];
    enforce!RIE(b.state != RowBlockState.invalid, "Row block in invalid state");
    if (b.dataRows.length == 0)
        return;
    auto mapper = RowMapper!StrT(b.rowDesc, resFcodes);
    foreach (dr; b.dataRows)
    {
        StrT tmp;
        mapper.map(dr, tmp, strict);
        or.put(tmp);
    }
}


T asType(T)(const QueryResult r, in FormatCode[] resFcodes = null)
{
    static struct ResS
    {
        T val;
    }
    return asStruct!ResS(r, false, resFcodes).val;
}

/// Returns array of preferrable format codes for the result wich perfectly
/// maps to struct.
FormatCode[] fcodesForStruct(T)()
    if (is(T == struct))
{
    FormatCode[] result;
    alias allPubs = allPublicFields!T;
    result.length = allPubs.length;
    foreach (i, fmeta; allPubs)
        result[i] = fcodeForField!(T, fmeta.name);
    return result;
}



private:


template fcodeForField(StrT, string f)
{
    static if (hasUda!(StrT, f, PgType))
        private enum OID oid = getUda!(StrT, f, PgType);
    else
        private enum OID oid = oidForType!(typeof(__traits(getMember, StrT, f)));
    private enum FieldSpec fs = FieldSpec(oid, true);
    enum FormatCode fcodeForField = DefaultSerializer!fs.formatCode;
}


struct RowMapper(StrT)
    if (is(StrT == struct))
{
    private const FormatCode[] m_resFcodes;

    this(const RowDescription rd, const FormatCode[] resFcodes = null)
    {
        if (!rd.isSet)
        {
            m_resFcodes = resFcodes;
            return;
        }
        auto tres = new FormatCode[rd.fieldCount];
        foreach (i, col; rd[].enumerate())
        {
            tres[i] = col.formatCode;
        }
        m_resFcodes = tres;
    }

    void map(immutable(ubyte)[] row, ref StrT dest, bool strict) @trusted
    {
        short colCount = deserializeNumber!short(row[0 .. 2]);
        row = row[2 .. $];

        foreach (i, fmeta; allPublicFields!StrT)
        {
            enum bool nullable = isNullable!(fmeta.type);
            int len = deserializeNumber(row[0 .. 4]);
            row = row[4..$];
            FormatCode fcode = m_resFcodes.length > i ? m_resFcodes[i] : FormatCode.Text;
            static if (hasUda!(StrT, fmeta.name, PgType))
            {
                // there is an OID override on this field
                enum OID oid = getUda!(StrT, fmeta.name, PgType);
                enum FieldSpec fs = FieldSpec(oid, nullable);
                alias NativeT = DefaultSerializer!fs.type;
                static if (is(fmeta.type == NativeT))
                {
                    DefaultSerializer!fs.deserialize(row, fcode, len,
                        &__traits(getMember, dest, fmeta.name));
                }
                else
                {
                    NativeT temp;
                    DefaultSerializer!fs.deserialize(row, fcode, len, &temp);
                    __traits(getMember, dest, fmeta.name) = temp.to!(fmeta.type);
                }
            }
            else
            {
                // we assume OID from the target type
                enum OID assumedOid = oidForType!(fmeta.type);
                enum FieldSpec fs = FieldSpec(assumedOid, nullable);
                alias NativeT = DefaultSerializer!fs.type;
                static assert (is(fmeta.type == NativeT));
                DefaultSerializer!fs.deserialize(row, fcode, len,
                    &__traits(getMember, dest, fmeta.name));
            }
            row = row[max(0, len) .. $];
            colCount--;
        }
        enforce!RIE(!strict || colCount == 0, "Not all columns were assigned");
    }
}

unittest
{
    struct TestTtruct
    {
        bool row1;
        int row2;
        long row3;
        double row4;
        string row5;
        Nullable!string row6;
        string row7;
    }
    RowMapper!TestTtruct mapper;
}

