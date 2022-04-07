using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Routing;

namespace Raven.Server.Web.Studio.Sharding
{
    public class ShardedStudioTasksHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/studio-tasks/periodic-backup/test-credentials", "POST")]
        public async Task TestPeriodicBackupCredentials()
        {
            await StudioTasksHandler.TestPeriodicBackupCredentials(this);
        }
    }
}
