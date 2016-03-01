using System;

namespace Raven.Server.ServerWide.Context
{
    public interface IMemoryContextPool : IDisposable
    {
        IDisposable AllocateOperationContext(out MemoryOperationContext context);
    }
}