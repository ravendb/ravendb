using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Processors.SchemaValidation;

internal abstract class AbstractSchemaValidationHandlerProcessorForPost<TRequestHandler, TOperationContext> : AbstractHandlerProcessorForUpdateDatabaseConfiguration<BlittableJsonReaderObject, TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractSchemaValidationHandlerProcessorForPost([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override void OnBeforeUpdateConfiguration(ref BlittableJsonReaderObject configuration, JsonOperationContext context)
    {
        RequestHandler.ServerStore.LicenseManager.AssertCanAddSchemaValidation();

        if (RavenLogManager.Instance.IsAuditEnabled)
            RequestHandler.LogAuditForDatabase("PUT", "Update schema validation configuration.");

        base.OnBeforeUpdateConfiguration(ref configuration, context);
    }

    protected override Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, BlittableJsonReaderObject configuration, string raftRequestId)
    {
        return RequestHandler.ServerStore.ModifySchemaValidation(context, RequestHandler.DatabaseName, configuration, raftRequestId);
    }
}
