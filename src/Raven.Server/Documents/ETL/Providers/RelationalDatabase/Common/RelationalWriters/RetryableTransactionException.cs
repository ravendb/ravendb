using System;

namespace Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common.RelationalWriters;

public sealed class RetryableTransactionException : Exception
{
    public RetryableTransactionException(Exception innerException)
        : base(innerException?.Message, innerException)
    {
    }
}
