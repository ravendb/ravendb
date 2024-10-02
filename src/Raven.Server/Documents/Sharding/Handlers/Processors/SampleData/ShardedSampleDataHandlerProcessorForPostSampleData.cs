using System.IO;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents.Handlers.Processors.SampleData;
using Raven.Server.Documents.Sharding.Handlers.Processors.Collections;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.SampleData
{
    internal sealed class ShardedSampleDataHandlerProcessorForPostSampleData : AbstractSampleDataHandlerProcessorForPostSampleData<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedSampleDataHandlerProcessorForPostSampleData([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask ExecuteSmugglerAsync(JsonOperationContext context, Stream sampleDataStream, DatabaseItemType operateOnTypes)
        {
            var operationId = RequestHandler.DatabaseContext.Operations.GetNextOperationId();
            var record = RequestHandler.DatabaseContext.DatabaseRecord;
            var options = new DatabaseSmugglerOptionsServerSide(RequestHandler.GetAuthorizationStatusForSmuggler(RequestHandler.DatabaseName))
            {
                OperateOnTypes = operateOnTypes,
                SkipRevisionCreation = true
            };

            using (var source = new OrchestratorStreamSource(sampleDataStream, context, RequestHandler.DatabaseName, RequestHandler.DatabaseContext.ShardCount, options))
            {
                var smuggler = new ShardedDatabaseSmuggler(
                    source,
                    new MultiShardedDestination(source, RequestHandler.DatabaseContext, RequestHandler, operationId),
                    context,
                    record,
                    RequestHandler.ServerStore,
                    options,
                    result: null);

                await smuggler.ExecuteAsync();
            }
        }

        protected override async ValueTask<bool> IsDatabaseEmptyAsync()
        {
            var op = new ShardedCollectionStatisticsOperation(HttpContext);
            var stats = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op);

            return stats.Collections.Count == 0;
        }
    }
}
