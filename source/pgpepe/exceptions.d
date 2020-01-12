module pgpepe.exceptions;

public import dpeq.exceptions;


class PgpepeException
{
    mixin basicExceptionCtors;
}

/// Thrown by socket stream and interpreted as terminal transport failure, causing
/// connection to be closed and disposed of. Timeouts are one of the causes of this exception.
class TransportException: PgpepeException
{
    mixin basicExceptionCtors;
}

/// Thrown when connector's transaction queue is full.
class TransactionLimitException: PgpepeException
{
    mixin basicExceptionCtors;
}

/// Thrown when query result is not what it was assumed to be by the calling code.
class ResultInterpretException: PgpepeException
{
    mixin basicExceptionCtors;
}

class UnexpectedRowCount: ResultInterpretException
{
    mixin basicExceptionCtors;
}