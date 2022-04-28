using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Refresh;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Refresh;

internal class RefreshHandlerProcessorForGetRefreshConfiguration : AbstractRefreshHandlerProcessorForGetRefreshConfiguration<DatabaseRequestHandler, DocumentsOperationContext>
{
    public RefreshHandlerProcessorForGetRefreshConfiguration([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override RefreshConfiguration GetRefreshConfiguration()
    {
        using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            using (var recordRaw = RequestHandler.ServerStore.Cluster.ReadRawDatabaseRecord(context, RequestHandler.Database.Name))
                return recordRaw?.RefreshConfiguration;
        }
    }
}
