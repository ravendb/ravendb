using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.SchemaValidation;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.SchemaValidation;

internal abstract class AbstractSchemaValidationHandlerProcessorForGet<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractSchemaValidationHandlerProcessorForGet([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract SchemaValidationConfiguration GetSchemaValidationConfiguration();

    public override async ValueTask ExecuteAsync()
    {
        var schemaValidationConfig = GetSchemaValidationConfiguration();

        if (schemaValidationConfig != null)
        {
            using (RequestHandler.Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                context.Write(writer, schemaValidationConfig.ToJson());
            }
        }
        else
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
        }
    }
}
