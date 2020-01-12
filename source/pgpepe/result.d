module pgpepe.result;

import dpeq;
import pgpepe.exceptions;


/// Description of SELECT result.
struct NamedRowDescription
{
    this(RowDescription msg)
    {
        m_desc = msg;
        for (i, fd; m_desc.fieldDescriptions)
            m_columnName2columnIndex[fd.name] = i;
    }

    private RowDescription m_desc;
    private size_t[string] m_columnName2columnIndex;

    /// Get array of field descriptions.
    @property const(FieldDescription)[] fieldDescriptions() const
    {
        return m_desc.fieldDescriptions;
    }

    /// Get field description by it's column (starting from zero).
    const(FieldDescription) fieldByIndex(size_t idx) const
    {
        return m_desc.fieldDescriptions[idx];
    }

    /// Get field description by it's name.
    const(FieldDescription) fieldByName(string name) const
    {
        if (name !in m_columnName2columnIndex)
            throw new ResultInterpretException("No field " ~ name ~ " in the response");
        return m_desc.fieldDescriptions[m_columnName2columnIndex[name]];
    }
}


/// Result of sole SQL statement's execution.
struct CommandResult
{
    /// Was there an ErrorMessage in the backend response stream before CommandComplete?
    bool errorEncountered;
    /// Error that was received.
    NoticeOrError error;
    /// CommandComplete tag. For select statements it's 'SELECT 5', if select returned
    /// 5 rows. 'UPDATE 0', 'DELETE 1000' are also valid examples.
    string commandTag;
    /// If command was SELECT, this will be a pointer to result set description.
    const(NamedRowDescription)* rowDescription;
    /// If command was SELECT, received rows.
    DataRow[] rows;
}


/// Result of successfull prepared statement execution.
struct PreparedResult
{
    CommandResult commandResult;
    /// Status of in-progress transaction.
    TransactionStatus transactionStatus;
}

/// Result of successfull simple query
/// (may have multiple sql statements in one string) execution.
struct SimpleQueryResult
{
    CommandResult[] commandResults;
    /// Status of in-progress transaction.
    TransactionStatus transactionStatus;
}