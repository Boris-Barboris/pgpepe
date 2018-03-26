module pgpepe.internal.typemap;

import std.typecons: Nullable;
import std.traits;
import std.uuid: UUID;

import dpeq;


FieldSpec specForType(NullableT)()
    if (isInstanceOf!(Nullable, NullableT))
{
    alias T = TemplateArgsOf!NullableT;
    return FieldSpec(oidForType!T(), true);
}

FieldSpec specForType(T)()
    if (!isInstanceOf!(Nullable, T))
{
    return FieldSpec(oidForType!T(), false);
}

OID oidForType(NullableT)()
    if (isInstanceOf!(Nullable, NullableT))
{
    alias T = TemplateArgsOf!NullableT;
    static assert (!isInstanceOf!(Nullable, T), "nested nullable");
    return oidForType!T();
}

OID oidForType(T)()
    if (!isInstanceOf!(Nullable, T))
{
    static if (is(T == bool))
        return PgType.BOOLEAN;
    else static if (is(T == long))
        return PgType.BIGINT;
    else static if (is(T == int))
        return PgType.INT;
    else static if (is(T == short))
        return PgType.SMALLINT;
    else static if (is(T == string))
        return PgType.VARCHAR;
    else static if (is(T == UUID))
        return PgType.UUID;
    else static if (is(T == float))
        return PgType.REAL;
    else static if (is(T == double))
        return PgType.DOUBLE;
    else
        assert (0, "Unsupported type");
}

unittest
{
    static assert (oidForType!(Nullable!double) == PgType.DOUBLE);
    static assert (oidForType!double == PgType.DOUBLE);
}