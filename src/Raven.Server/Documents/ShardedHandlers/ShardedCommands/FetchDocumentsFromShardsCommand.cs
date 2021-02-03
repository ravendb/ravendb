using System;
using System.Collections.Generic;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ShardedHandlers.ShardedCommands
{
    public class FetchDocumentsFromShardsCommand : ShardedCommand, IDisposable
    {
        private readonly IDisposable _disposable;
        public TransactionOperationContext Context;
        public List<int> PositionMatches;

        public FetchDocumentsFromShardsCommand(ShardedRequestHandler handler) : base(handler, ShardedCommands.Headers.None)
        {
            _disposable = handler.ContextPool.AllocateOperationContext(out Context);
        }

        public void Dispose()
        {
            _disposable?.Dispose();
        }
    }
}
