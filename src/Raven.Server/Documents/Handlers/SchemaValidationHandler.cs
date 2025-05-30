using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.SchemaValidation;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers;

public sealed class SchemaValidationHandler : DatabaseRequestHandler
{
    [RavenAction("/databases/*/schema-validation/config", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task GetSchemaValidationConfig()
    {
        using (var processor = new SchemaValidationHandlerProcessorForGet(this))
            await processor.ExecuteAsync();
    }

    [RavenAction("/databases/*/admin/schema-validation/config", "POST", AuthorizationStatus.DatabaseAdmin)]
    public async Task ConfigSchemaValidation()
    {
        using (var processor = new SchemaValidationHandlerProcessorForPost(this))
            await processor.ExecuteAsync();
    }
}
