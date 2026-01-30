using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.Web.Studio.Processors;

namespace Raven.Server.Web.Studio;

public sealed class StudioDocumentHandler : DatabaseRequestHandler
{
    [RavenAction("/databases/*/studio/validate-schema", "POST", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task ValidateSchema()
    {
        using (var processor = new StudioDocumentHandlerProcessorForValidateDocument(this))
            await processor.ExecuteAsync();
    }
}
