using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Studio.Processors;

namespace Raven.Server.Web.Studio.Sharding
{
    public class ShardedStudioTasksHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/studio-tasks/periodic-backup/test-credentials", "POST")]
        public async Task TestPeriodicBackupCredentials()
        {
            using (var processor = new StudioTasksHandlerProcessorForTestPeriodicBackupCredentials<ShardedDatabaseRequestHandler, TransactionOperationContext>(this, ContextPool))
                await processor.ExecuteAsync();
        }
    }
}
