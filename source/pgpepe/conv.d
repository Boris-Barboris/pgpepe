module pgpepe.conv;

import std.algorithm: max;
import std.conv;
import std.exception: enforce;

import dpeq;
public import dpeq.result: QueryResult;

import pgpepe.exceptions;
import pgpepe.internal.meta;
import pgpepe.internal.typemap;



alias RIE = ResultInterpretException;

/** Get command tag count from the result. For example, UPDATE returns
command tag "UPDATE rowcount". This function returns rowcount as int. */
int asTag(scope const QueryResult r) @safe pure
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


struct RowMapper(StrT)
    if (is(StrT == struct))
{
    this(const RowDescription rd) @safe
    {
        // TODO:
    }

    void map(immutable(ubyte)[] row, ref StrT dest, bool strict) @trusted pure
    {
        short colCount = deserializeNumber!short(row[0 .. 2]);
        row = row[2 .. $];

        foreach (fmeta; allPublicFields!StrT)
        {
            enum bool nullable = isNullable!(fmeta.type);
            int len = deserializeNumber(row[0 .. 4]);
            row = row[4..$];
            static if (hasUda!(StrT, fmeta.name, PgType))
            {
                // there is an OID override on this field
                enum OID oid = getUda!(StrT, fmeta.name, PgType);
                enum FieldSpec fs = FieldSpec(oid, nullable);
                alias NativeT = DefaultSerializer!fs.type;
                static if (is(fmeta.type == NativeT))
                {
                    DefaultSerializer!fs.deserialize(row, FormatCode.Text, len,
                        &__traits(getMember, dest, fmeta.name));
                }
                else
                {
                    NativeT temp;
                    DefaultSerializer!fs.deserialize(
                        row, FormatCode.Text, len, &temp);
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
                DefaultSerializer!fs.deserialize(row, FormatCode.Text, len,
                    &__traits(getMember, dest, fmeta.name));
            }
            row = row[max(0, len) .. $];
            colCount--;
        }
        enforce!RIE(!strict || colCount == 0, "Not all columns were assigned");
    }
}


StrT asStruct(StrT)(const QueryResult r, bool strict = true) @trusted pure
{
    enforce!RIE(r.blocks.length == 1, "Expected one row block");
    const RowBlock b = r.blocks[0];
    enforce!RIE(b.state == RowBlockState.complete, "Row block not in complete state");
    enforce!UnexpectedRowCount(b.dataRows.length == 1, "Expected one row");
    StrT res;
    auto mapper = RowMapper!StrT(b.rowDesc);
    mapper.map(b.dataRows[0], res, strict);
    return res;
}

StrT[] asStructs(StrT)(const QueryResult r, bool strict = true) @trusted pure
{
    enforce!RIE(r.blocks.length == 1, "Expected one row block");
    const RowBlock b = r.blocks[0];
    enforce!RIE(b.state != RowBlockState.invalid, "Row block in invalid state");
    StrT[] res;
    if (b.dataRows.length == 0)
        return res;
    res.length = b.dataRows.length;
    auto mapper = RowMapper!StrT(b.rowDesc);
    foreach (i, ref s; res)
        mapper.map(b.dataRows[i], s, strict);
    return res;
}

