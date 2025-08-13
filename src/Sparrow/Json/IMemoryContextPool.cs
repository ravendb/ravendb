using System;

namespace Sparrow.Json;

public interface IMemoryContextPool : IDisposable
{
    IDisposable AllocateOperationContext(out JsonOperationContext context);
}
