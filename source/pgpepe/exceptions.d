module pgpepe.exceptions;

public import dpeq.exceptions;


/// Thrown when connector is too busy
class TransactionLimitReached: Exception
{
    mixin ExceptionConstructors;
}