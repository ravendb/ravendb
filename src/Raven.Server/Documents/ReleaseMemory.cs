using System;
using Raven.Server.ServerWide.Context;
using Sparrow;

namespace Raven.Server.Documents
{
    public struct ReleaseMemory : IDisposable
    {
        private ByteString _allocation;
        private DocumentsOperationContext _ctx;

        public ReleaseMemory(ByteString allocation, DocumentsOperationContext ctx)
        {
            _allocation = allocation;
            _ctx = ctx;
        }

        public void Dispose()
        {
            if (_allocation.HasValue)
                _ctx.Allocator.Release(ref _allocation);
        }
    }
}