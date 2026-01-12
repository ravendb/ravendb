using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Routing;
using Raven.Server.Web.Studio.Processors;

namespace Raven.Server.Web.Studio.Sharding;

public sealed class ShardedStudioDocumentHandler : ShardedDatabaseRequestHandler
{
    [RavenAction("/databases/*/studio/validate-schema", "POST", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task ValidateSchema()
    {
        using (var processor = new ShardedStudioDocumentHandlerProcessorForValidateDocument(this))
            await processor.ExecuteAsync();
    }
}
