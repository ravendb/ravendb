using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Revisions;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedRevisionsHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/revisions", "GET")]
        public async Task GetRevisionsFor()
        {
            using (var processor = new ShardedRevisionsHandlerProcessorForGetRevisions(this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenShardedAction("/databases/*/revisions/count", "GET")]
        public async Task GetRevisionsCountFor()
        {
            using (var processor = new ShardedRevisionsHandlerProcessorForGetRevisionsCount(this))
            {
                await processor.ExecuteAsync();
            }
        }
    }
}
