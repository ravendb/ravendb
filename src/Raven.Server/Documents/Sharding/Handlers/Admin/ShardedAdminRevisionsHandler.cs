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

        [RavenShardedAction("/databases/*/admin/revisions/conflicts/config", "POST")]
        public async Task ConfigConflictedRevisions()
        {
            using (var processor = new ShardedAdminRevisionsHandlerProcessorForPostRevisionsConflictsConfiguration(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/revisions/config/enforce", "POST")]
        public async Task EnforceConfigRevisions()
        {
            using (var processor = new ShardedAdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration(this))
                await processor.ExecuteAsync();
        }
    }
}
