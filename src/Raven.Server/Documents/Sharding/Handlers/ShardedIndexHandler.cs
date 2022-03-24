using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Documents.Handlers.Processors.Indexes;
using Raven.Server.Documents.Sharding.Handlers.Processors.Indexes;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedIndexHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/indexes", "GET")]
        public async Task GetAll()
        {
            var namesOnly = GetBoolValueQueryString("namesOnly", required: false) ?? false;

            if (namesOnly)
            {
                using (var processor = new ShardedIndexHandlerProcessorForGetAllNames(this))
                    await processor.ExecuteAsync();

                return;
            }

            using (var processor = new ShardedIndexHandlerProcessorForGetAll(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/indexes/stats", "GET")]
        public async Task Stats()
        {
            using (var processor = new ShardedIndexHandlerProcessorForGetDatabaseIndexStatistics(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/indexes/staleness", "GET")]
        public async Task Stale()
        {
            using (var processor = new ShardedIndexHandlerProcessorForStale(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/indexes/progress", "GET")]
        public async Task Progress()
        {
            using (var processor = new ShardedIndexHandlerProcessorForProgress(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/indexes", "RESET")]
        public async Task Reset()
        {
            using (var processor = new ShardedIndexHandlerProcessorForReset(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/indexes/performance", "GET")]
        public async Task Performance()
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "Implement it for the Client API");

            var shard = GetLongQueryString("shard", false);
            if (shard == null)
                throw new InvalidOperationException("In a sharded environment you must provide a shard id");

            if (DatabaseContext.RequestExecutors.Length <= shard)
                throw new InvalidOperationException($"Non existing shard id, {shard}");

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var executor = DatabaseContext.RequestExecutors[shard.Value];
                var command = new GetIndexPerformanceStatisticsOperation.GetIndexPerformanceStatisticsCommand(null, (int)shard.Value);
                await executor.ExecuteAsync(command, context);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WritePerformanceStats(context, command.Result);
                }
            }
        }

        [RavenShardedAction("/databases/*/indexes/set-lock", "POST")]
        public async Task SetLockMode()
        {
            using (var processor = new ShardedIndexHandlerProcessorForSetLockMode(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/indexes/set-priority", "POST")]
        public async Task SetPriority()
        {
            using (var processor = new ShardedIndexHandlerProcessorForSetPriority(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/indexes/errors", "DELETE")]
        public async Task ClearErrors()
        {
            using (var processor = new ShardedIndexHandlerProcessorForClearErrors(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/indexes/errors", "GET")]
        public async Task GetErrors()
        {
            using (var processor = new ShardedIndexHandlerProcessorForGetErrors(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/indexes/status", "GET")]
        public async Task Status()
        {
            using (var processor = new ShardedIndexHandlerProcessorForGetIndexesStatus(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/index/open-faulty-index", "POST")]
        public async Task OpenFaultyIndex()
        {
            using (var processor = new ShardedIndexHandlerProcessorForOpenFaultyIndex(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/indexes", "DELETE")]
        public async Task Delete()
        {
            using (var processor = new ShardedIndexHandlerProcessorForDelete(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/indexes/c-sharp-index-definition", "GET")]
        public async Task GenerateCSharpIndexDefinition()
        {
            using (var processor = new ShardedIndexProcessorForGenerateCSharpIndexDefinition(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/indexes/history", "GET")]
        public async Task GetIndexHistory()
        {
            using (var processor = new IndexHandlerProcessorForGetIndexHistory<TransactionOperationContext>(this, ContextPool, DatabaseContext.DatabaseName))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/indexes/source", "GET")]
        public async Task Source()
        {
            using (var processor = new ShardedIndexHandlerProcessorForSource(this))
                await processor.ExecuteAsync();
        }
    }
}


