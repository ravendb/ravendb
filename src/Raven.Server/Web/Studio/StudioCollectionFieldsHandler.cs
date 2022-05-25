using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors.Studio;
using Raven.Server.Routing;

namespace Raven.Server.Web.Studio
{
    public class StudioCollectionFieldsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/studio/collections/fields", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetCollectionFields()
        {
            using (var processor = new StudioCollectionFieldsHandlerProcessorForGetCollectionFields(this))
                await processor.ExecuteAsync();
        }
    }
}
