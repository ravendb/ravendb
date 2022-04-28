using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.Http;
using Raven.Server.Web.Operations;
using Raven.Server.Web.Studio.Processors;

namespace Raven.Server.Web.Studio.Sharding.Processors
{
    internal class ShardedStudioDatabaseTasksHandlerProcessorForGetSuggestConflictResolution : AbstractStudioDatabaseTasksHandlerProcessorForGetSuggestConflictResolution<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedStudioDatabaseTasksHandlerProcessorForGetSuggestConflictResolution([NotNull] ShardedDatabaseRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override async Task GetSuggestConflictResolutionAsync(TransactionOperationContext context, string documentId)
        {
            var cmd = new GetSuggestConflictResolutionOperation.GetSuggestConflictResolutionCommand(documentId);
            var proxyCommand = new ProxyCommand<ConflictResolverAdvisor.MergeResult>(cmd, RequestHandler.HttpContext.Response);

            var shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, documentId);
            await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(proxyCommand, shardNumber);
        }
    }
}
