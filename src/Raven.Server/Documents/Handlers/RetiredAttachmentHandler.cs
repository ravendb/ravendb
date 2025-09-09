using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Attachments.Retired;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers
{
    public sealed class RetiredAttachmentHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/attachments/retire/config", "GET", AuthorizationStatus.DatabaseAdmin)]
        public async Task GetRetireConfig()
        {
            using (var processor = new RetiredAttachmentHandlerProcessorForGetRetireConfig(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/attachments/retire/config", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task AddRetireConfig()
        {
            using (var processor = new RetiredAttachmentHandlerProcessorForAddRetireConfig(this))
                await processor.ExecuteAsync();
        }
    }
}
