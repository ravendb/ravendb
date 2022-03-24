using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedRachisDatabaseHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/admin/rachis/wait-for-raft-commands", "POST")]
        public async Task WaitFor()
        {
            using (var processor = new ShardedRachisHandlerProcessorForWaitForRaftCommands(this))
            {
                await processor.ExecuteAsync();
            }
        }
    }
}
