using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Routing;
using Raven.Server.Web.Studio.Sharding.Processors;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public sealed class ShardedSingleDatabaseAdminHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/admin/restart", "POST")]
        public async Task RestartDatabase()
        {
            using (var processor = new ShardedDatabaseTasksHandlerProcessorForRestartDatabase(this, "/admin/restart"))
                await processor.ExecuteAsync();
        }
    }
}
