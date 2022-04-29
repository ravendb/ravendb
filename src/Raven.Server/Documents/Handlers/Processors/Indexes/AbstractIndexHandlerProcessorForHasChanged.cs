using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Indexes;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal abstract class AbstractIndexHandlerProcessorForHasChanged<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractIndexHandlerProcessorForHasChanged([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract AbstractIndexHasChangedController GetHasChangedController();

    public override async ValueTask ExecuteAsync()
    {
        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        using (var json = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "index/definition"))
        {
            var indexDefinition = JsonDeserializationServer.IndexDefinition(json);

            if (indexDefinition?.Name == null || indexDefinition.Maps.Count == 0)
                throw new BadRequestException("Index definition must contain name and at least one map.");

            var controller = GetHasChangedController();
            var changed = controller.HasChanged(indexDefinition);

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Changed");
                writer.WriteBool(changed);
                writer.WriteEndObject();
            }
        }
    }
}
