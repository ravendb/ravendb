using System;

namespace Raven.Server.ServerWide.Context
{
    public interface IDocumentsContextPool : IMemoryContextPool
    {
        IDisposable AllocateOperationContext(out DocumentsOperationContext context);
    }
}