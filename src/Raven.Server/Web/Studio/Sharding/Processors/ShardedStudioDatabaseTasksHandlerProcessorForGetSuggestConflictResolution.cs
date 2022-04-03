using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Studio.Processors;
using Raven.Server.Web.Studio.Sharding.Commands;

namespace Raven.Server.Web.Studio.Sharding.Processors
{
    internal class ShardedStudioDatabaseTasksHandlerProcessorForGetSuggestConflictResolution : AbstractStudioDatabaseTasksHandlerProcessorForGetSuggestConflictResolution<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedStudioDatabaseTasksHandlerProcessorForGetSuggestConflictResolution([NotNull] ShardedDatabaseRequestHandler requestHandler)
            : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async Task GetSuggestConflictResolutionAsync(TransactionOperationContext context, string documentId)
        {
            var cmd = new GetConflictResolutionCommand(RequestHandler, documentId);

            var shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, documentId);
            await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(cmd, shardNumber);
        }
    }
}
