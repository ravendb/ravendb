using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Admin.Processors.Revisions;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers.Admin
{
    internal class ShardedAdminRevisionsHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/admin/revisions", "DELETE")]
        public async Task DeleteRevisionsFor()
        {
            using (var processor = new ShardedAdminRevisionsHandlerProcessorForDeleteRevisions(this))
                await processor.ExecuteAsync();
        }
    }
}
