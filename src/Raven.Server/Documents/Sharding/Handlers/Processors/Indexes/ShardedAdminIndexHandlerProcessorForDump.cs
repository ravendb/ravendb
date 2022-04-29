using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Admin.Processors.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Indexes
{
    internal class ShardedAdminIndexHandlerProcessorForDump : AbstractAdminIndexHandlerProcessorForDump<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedAdminIndexHandlerProcessorForDump([NotNull] ShardedDatabaseRequestHandler requestHandler)
            : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override bool SupportsCurrentNode => false;

        protected override ValueTask ExecuteForCurrentNodeAsync() => throw new NotSupportedException();

        protected override Task ExecuteForRemoteNodeAsync(ProxyCommand command)
        {
            var shardNumber = GetShardNumber();

            return RequestHandler.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber);
        }
    }
}
