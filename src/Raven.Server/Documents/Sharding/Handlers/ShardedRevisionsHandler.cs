using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Revisions;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public sealed class ShardedRevisionsHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/revisions", "GET")]
        public async Task GetRevisionsFor()
        {
            using (var processor = new ShardedRevisionsHandlerProcessorForGetRevisions(this)) 
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/revisions/count", "GET")]
        public async Task GetRevisionsCountFor()
        {
            using (var processor = new ShardedRevisionsHandlerProcessorForGetRevisionsCount(this)) 
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/revisions/config", "POST")]
        public async Task PostRevisionsConfiguration()
        {
            using (var processor = new ShardedRevisionsHandlerProcessorForPostRevisionsConfiguration(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/revisions/config", "GET")]
        public async Task GetRevisionsConfiguration()
        {
            using (var processor = new ShardedRevisionsHandlerProcessorForGetRevisionsConfiguration(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/revisions/bin", "GET")]
        public async Task GetRevisionsBin()
        {
            using (var processor = new ShardedRevisionsHandlerProcessorForGetRevisionsBin(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/revisions/conflicts/config", "GET")]
        public async Task GetConflictRevisionsConfig()
        {
            using (var processor = new ShardedRevisionsHandlerProcessorForGetRevisionsConflictsConfiguration(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/revisions/revert", "POST")]
        public async Task Revert()
        {
            using (var processor = new ShardedRevisionsHandlerProcessorForRevertRevisions(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/revisions/revert/docs", "POST")]
        public async Task RevertDocument()
        {
            using (var processor = new ShardedRevisionsHandlerProcessorForRevertRevisionsForDocument(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/revisions/resolved", "GET")]
        public async Task GetResolvedConflictsSince()
        {
            using (var processor = new ShardedRevisionsHandlerProcessorForGetResolvedRevisions(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/debug/documents/get-revisions", "GET")]
        public async Task GetRevisions()
        {
            using (var processor = new ShardedRevisionsHandlerProcessorForGetRevisionsDebug(this))
                await processor.ExecuteAsync();
        }
    }
}
