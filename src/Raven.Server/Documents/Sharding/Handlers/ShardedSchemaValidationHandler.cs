using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.SchemaValidation;
using Raven.Server.Documents.Sharding.Handlers.Processors.SchemaValidation;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers;

public sealed class ShardedSchemaValidationHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/schema-validation/config", "GET")]
    public async Task GetSchemaValidationConfig()
    {
        using (var processor = new ShardedSchemaValidationHandlerProcessorForGet(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/admin/schema-validation/config", "POST")]
    public async Task ConfigSchemaValidation()
    {
        using (var processor = new ShardedSchemaValidationHandlerProcessorForPost(this))
            await processor.ExecuteAsync();
    }
    
    [RavenShardedAction("/databases/*/schema-validation/validate", "POST")]
    public async Task ValidateSchema()
    {
        using (var processor = new ShardedSchemaValidationHandlerProcessorForValidate(this))
            await processor.ExecuteAsync();
    }
}
