using JetBrains.Annotations;
using Raven.Client.Documents.Operations.SchemaValidation;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.SchemaValidation;

internal sealed class SchemaValidationHandlerProcessorForGet : AbstractSchemaValidationHandlerProcessorForGet<DatabaseRequestHandler, DocumentsOperationContext>
{
    public SchemaValidationHandlerProcessorForGet([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override SchemaValidationConfiguration GetSchemaValidationConfiguration()
    {
        using (RequestHandler.Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            SchemaValidationConfiguration schemaConfig;
            using (var recordRaw = RequestHandler.Server.ServerStore.Cluster.ReadRawDatabaseRecord(context, RequestHandler.Database.Name))
            {
                schemaConfig = recordRaw?.SchemaValidationConfiguration;
            }

            return schemaConfig;
        }
    }
}
