using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Attachments.Remote;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers
{
    public sealed class RemoteAttachmentHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/attachments/remote/config", "GET", AuthorizationStatus.DatabaseAdmin)]
        public async Task GetRemoteConfig()
        {
            using (var processor = new RemoteAttachmentHandlerProcessorForGetRemoteConfig(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/attachments/remote/config", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task AddRemoteConfig()
        {
            using (var processor = new RemoteAttachmentHandlerProcessorForAddRemoteConfig(this))
                await processor.ExecuteAsync();
        }
    }
}
