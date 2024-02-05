using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.Web.Http;

using Raven.Server.Documents.Handlers.Processors.Debugging;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Debugging
{
    internal sealed class ShardedStorageHandlerProcessorForGetScratchBufferReport : AbstractStorageHandlerProcessorForGetEnvironmentReport<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedStorageHandlerProcessorForGetScratchBufferReport([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override bool SupportsCurrentNode => false;

        protected override ValueTask HandleCurrentNodeAsync() => throw new NotSupportedException();

        protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token)
        {
            var shardNumber = GetShardNumber();

            return RequestHandler.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber, token.Token);
        }
    }
}
