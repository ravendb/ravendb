using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors
{
    internal class ShardedSampleDataHandlerProcessorForPostSampleData<TRequestHandler, TOperationContext> : AbstractSampleDataHandlerProcessorForPostSampleData<ShardedRequestHandler, TransactionOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        public ShardedSampleDataHandlerProcessorForPostSampleData([NotNull] ShardedRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override string GetDatabaseName()
        {
            return RequestHandler.ShardedContext.DatabaseName;
        }

        protected override async ValueTask WaitForIndexNotificationAsync(long index)
        {
            await RequestHandler.ServerStore.Cluster.WaitForIndexNotification(index, RequestHandler.ServerStore.Engine.OperationTimeout);
        }

        protected override async ValueTask ExecuteSmugglerAsync(JsonOperationContext context, ISmugglerSource source, Stream sampleData, DatabaseItemType operateOnTypes)
        {
            var record = RequestHandler.ShardedContext.DatabaseRecord;

            var smuggler = new ShardedDatabaseSmuggler(source, context, record,
                RequestHandler.Server.ServerStore, RequestHandler.ShardedContext, RequestHandler,
                options: new DatabaseSmugglerOptionsServerSide
                {
                    OperateOnTypes = operateOnTypes, 
                    SkipRevisionCreation = true
                }, null);

            await smuggler.ExecuteAsync();
        }

        protected override async ValueTask<bool> IsDatabaseEmptyAsync()
        {
            var op = new ShardedCollectionHandler.ShardedCollectionStatisticsOperation();
            var stats = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op);

            return stats.Collections.Count == 0;
        }
    }
}
