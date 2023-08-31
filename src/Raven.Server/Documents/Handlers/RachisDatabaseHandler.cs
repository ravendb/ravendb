using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Rachis;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers
{
    public sealed class RachisDatabaseHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/rachis/wait-for-index-notifications", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task WaitForIndexNotifications()
        {
            using (var processor = new RachisHandlerProcessorForWaitForIndexNotifications(this))
            {
                await processor.ExecuteAsync();
            }
        }
    }
}
