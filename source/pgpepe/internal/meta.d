module pgpepe.internal.meta;

public import std.meta: Filter, anySatisfy, staticMap, aliasSeqOf, AliasSeq;
public import std.traits: FieldNameTuple, Unqual;
public import painlesstraits: hasAnnotation, getAnnotation;


/** Assignable field descriptor. */
struct FieldMeta(T, string field_name)
{
    alias type = T;
    enum string name = field_name;
}


/** This template allows to qeury field names and types of composites
(structs or classes). */
template allPublicFields(T)
{
    private template fieldNameToMeta(string field)
    {
        alias fieldNameToMeta = FieldMeta!(typeOfMember!(T, field), field);
    }
    alias allPublicFields = staticMap!(fieldNameToMeta, publicFieldNames!T);
    static assert(allPublicFields.length > 0, "No fields for type " ~ T.stringof);
}


/** Defines filtering function to pass only for members with Attr UDA on them. */
template HasUdaFilter(T, alias Attr)
{
    template filter(alias field_meta)
    {
        enum filter = hasAnnotation!(__traits(getMember, T, field_meta.name), Attr);
    }
}


template hasUda(T, string field, alias Attr)
{
    enum hasUda = hasAnnotation!(__traits(getMember, T, field), Attr);
}


template getUda(T, string field, alias Attr)
{
    enum getUda = getAnnotation!(__traits(getMember, T, field), Attr);
}


/** Returns alias sequence of field descriptors that have Attr UDA on them. */
template allFieldsWithUda(T, alias Attr)
{
    alias allFieldsWithUda = Filter!(HasUdaFilter!(T, Attr).filter, allFields!T);
}


/** For convenience */
alias allFieldNames = FieldNameTuple;

template publicFieldNames(T)
{
    alias publicFieldNames = Filter!(PublicFilter!T.filter, allFieldNames!T);
}

template PublicFilter(T)
{
    template filter(string attrName)
    {
        enum bool filter =
            __traits(getProtection, __traits(getMember, T, attrName)) == "public";
    }
}


/** Returns type of the Owner field named member */
template typeOfMember(Owner, string member)
{
    alias typeOfMember = typeof(__traits(getMember, Owner, member));
}


/** Filter wich passes when element is found in T tuple */
template CanFind(alias needle)
{
    template In(Haystack...)
    {
        private template EqualPred(alias v)
        {
            enum EqualPred = (v == needle);
        }
        enum In = anySatisfy!(EqualPred, Haystack);
    }
}

unittest
{
    static assert (CanFind!"a".In!(AliasSeq!("b", "a")));
}


/** Template intersection */
template Intersect(T1...)
{
    template With(T2...)
    {
        private template IntersectFilt(alias val)
        {
            enum IntersectFilt = CanFind!val.In!T2;
        }
        alias With = Filter!(IntersectFilt, T1);
    }
}

unittest
{
    static assert (Intersect!("n1", "n2").With!("n2", "n3") == AliasSeq!("n2"));
}
