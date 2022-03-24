using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers
{
    public class RachisDatabaseHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/rachis/wait-for-raft-commands", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task WaitForRaftCommands()
        {
            using (var processor = new RachisHandlerProcessorForWaitForRaftCommands(this))
            {
                await processor.ExecuteAsync();
            }
        }
    }
}
