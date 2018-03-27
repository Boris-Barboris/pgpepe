module pgpepe.exceptions;

public import dpeq.exceptions;


/// Thrown when connector is too busy
class TransactionLimitException: Exception
{
    mixin ExceptionConstructors;
}

/// Thrown when query result is not what it was assumed to be
class ResultInterpretException: Exception
{
    mixin ExceptionConstructors;
}

class UnexpectedRowCount: ResultInterpretException
{
    mixin ExceptionConstructors;
}