module pgpepe.constants;

import std.container.rbtree;


enum IsolationLevel: byte
{
    READ_COMMITTED,
    REPEATABLE_READ,
    SERIALIZABLE
}


/// Transaction attributes that are respected by pgpepe.
struct TsacConfig
{
    /// Requested isolation level.
    IsolationLevel isolation = IsolationLevel.READ_COMMITTED;
    /// READ ONLY clause of BEGIN.
    bool readonly = false;
    /// The DEFERRABLE transaction property has no effect unless the transaction is also SERIALIZABLE and READ ONLY. When all three of these properties are selected for a transaction, the transaction may block when first acquiring its snapshot, after which it is able to run without the normal overhead of a SERIALIZABLE transaction and without any risk of contributing to or being canceled by a serialization failure. This mode is well suited for long-running reports or backups.
    bool deferrable = false;
}


/// shortcut for read committed read-write
enum TsacConfig TC_READCOMMITTED = TsacConfig(IsolationLevel.READ_COMMITTED, false, false);
/// shortcut for repeatable read read-write
enum TsacConfig TC_REPEATABLEREAD = TsacConfig(IsolationLevel.REPEATABLE_READ, false, false);
/// shortcut for serializable read-write
enum TsacConfig TC_SERIAL = TsacConfig(IsolationLevel.SERIALIZABLE, false, false);


enum TransactionCommitState
{
    /// Commit was issued and transaction may or may not have been
    /// committed, the state is unknown.
    UNKNOWN,
    /// Commit was not issued or was rejected by backend, or the error during
    /// transaction initiated rollback.
    ROLLED_BACK,
    /// Commit succeeded.
    COMMITTED
}