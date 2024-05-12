using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors.Revisions;
using Raven.Server.Documents.Handlers.Processors.Studio;
using Raven.Server.Routing;
using Raven.Server.Web.Studio.Processors;

namespace Raven.Server.Web.Studio
{
    public sealed class StudioCollectionsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/studio/collections/preview", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task PreviewCollection()
        {
            using (var processor = new StudioCollectionsHandlerProcessorForPreviewCollection(this, Database))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/studio/collections/docs", "DELETE", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task Delete()
        {
            using (var processor = new StudioCollectionHandlerProcessorForDeleteCollection(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/studio/revisions/preview", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task PreviewRevisions()
        {
            using (var processor = new StudioCollectionsHandlerProcessorForPreviewRevisions(this))
                await processor.ExecuteAsync();
        }
    }
}
