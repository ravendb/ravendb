using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Indexes;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Documents.Sharding.Handlers.Processors.Indexes;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public sealed class ShardedIndexHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/indexes/replace", "POST")]
        public async Task Replace()
        {
            using (var processor = new ShardedIndexHandlerProcessorForReplace(this))
                await processor.ExecuteAsync();
        }

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
            using (var processor = new IndexHandlerProcessorForGetIndexHistory<TransactionOperationContext>(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/indexes/has-changed", "POST")]
        public async Task HasChanged()
        {
            using (var processor = new ShardedIndexHandlerProcessorForHasChanged(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/indexes/debug", "GET")]
        public async Task Debug()
        {
            using (var processor = new ShardedIndexHandlerProcessorForDebug(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/indexes/source", "GET")]
        public async Task Source()
        {
            using (var processor = new ShardedIndexHandlerProcessorForSource(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/indexes/performance", "GET")]
        public async Task Performance()
        {
            using (var processor = new ShardedIndexHandlerProcessorForPerformance(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/indexes/performance/live", "GET")]
        public async Task PerformanceLive()
        {
            using (var processor = new ShardedIndexHandlerProcessorForPerformanceLive(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/indexes/terms", "GET")]
        public async Task Terms()
        {
            using (var processor = new ShardedIndexHandlerProcessorForTerms(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/indexes/total-time", "GET")]
        public async Task TotalTime()
        {
            using (var processor = new ShardedIndexHandlerProcessorForTotalTime(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/indexes/suggest-index-merge", "GET")]
        public async Task SuggestIndexMerge()
        {
            using (var processor = new ShardedIndexHandlerProcessorForSuggestIndexMerge(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/indexes/try", "POST")]
        public async Task TestJavaScriptIndex()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support JavaScript indexes."))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/indexes/finish-rolling", "POST")]
        public async Task FinishRolling()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support PutRollingIndex command."))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/indexes/auto/convert", "GET")]
        public async Task ConvertAutoIndex()
        {
            using (var processor = new IndexHandlerProcessorForConvertAutoIndex<ShardedDatabaseRequestHandler, TransactionOperationContext>(this))
                await processor.ExecuteAsync();
        }
    }
}


