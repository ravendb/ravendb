using System;

namespace Raven.Server.ServerWide.Context
{
    public interface IFilesContextPool : IMemoryContextPool
    {
        IDisposable AllocateOperationContext(out FilesOperationContext context);
    }
}