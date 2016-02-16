using System;

namespace Raven.Server.ServerWide.Context
{
    public interface IDocumentsContextPool : ITransactionContextPool
    {
        IDisposable AllocateOperationContext(out DocumentsOperationContext context);
    }
}