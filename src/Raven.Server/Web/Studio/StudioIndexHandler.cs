using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors.Studio;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Studio.Processors;

namespace Raven.Server.Web.Studio
{
    public class StudioIndexHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/studio/indexes/errors-count", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetIndexErrorsCount()
        {
            using (var processor = new StudioIndexHandlerProcessorForGetIndexErrorsCount(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/studio/index-type", "POST", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task PostIndexType()
        {
            using (var processor = new StudioIndexHandlerForPostIndexType<DocumentsOperationContext>(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/studio/index-fields", "POST", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task PostIndexFields()
        {
            using (var processor = new StudioIndexHandlerForPostIndexFields(this))
                await processor.ExecuteAsync();
        }
    }
}
