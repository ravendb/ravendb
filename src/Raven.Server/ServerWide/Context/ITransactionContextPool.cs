using System;

namespace Raven.Server.ServerWide.Context
{
    public interface ITransactionContextPool : IMemoryContextPool
    {
        IDisposable AllocateOperationContext(out TransactionOperationContext context);
    }
}