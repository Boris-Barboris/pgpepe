module pgpepe.constants;

public import dpeq.constants: OID, PgType;


enum IsolationLevel: string
{
    READ_COMMITTED = "READ COMMITTED",
    REPEATABLE_READ = "REPEATABLE READ",
    SERIALIZABLE = "SERIALIZABLE"
}

immutable IsolationLevel READ_COMMITTED = IsolationLevel.READ_COMMITTED;
immutable IsolationLevel REPEATABLE_READ = IsolationLevel.REPEATABLE_READ;
immutable IsolationLevel SERIALIZABLE = IsolationLevel.SERIALIZABLE;


/// All possible types of transactions are described using this structure
struct TsacConfig
{
    IsolationLevel isolation = READ_COMMITTED;
    bool readonly = false;
    bool fast = true;
    bool deferrable = false;    // useful for serializable readonly
}

immutable TsacConfig TSAC_DEFAULT = TsacConfig(READ_COMMITTED, false, false, false);
immutable TsacConfig TSAC_FDEFAULT = TsacConfig(READ_COMMITTED, false, true, false);
immutable TsacConfig TSAC_FSERIAL = TsacConfig(SERIALIZABLE, false, true, false);
immutable TsacConfig TSAC_SERIAL = TsacConfig(SERIALIZABLE, false, false, false);
immutable TsacConfig TSAC_SDEFER = TsacConfig(SERIALIZABLE, true, false, true);