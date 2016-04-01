using System;
using Sparrow.Json;

namespace Raven.Server.ServerWide.Context
{
    public interface IMemoryContextPool : IDisposable
    {
        IDisposable AllocateOperationContext(out JsonOperationContext context);
    }
}