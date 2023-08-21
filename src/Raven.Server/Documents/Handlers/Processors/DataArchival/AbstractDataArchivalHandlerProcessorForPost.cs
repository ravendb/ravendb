using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.DataArchival;

internal abstract class AbstractDataArchivalHandlerProcessorForPost<TRequestHandler, TOperationContext> : AbstractHandlerProcessorForUpdateDatabaseConfiguration<BlittableJsonReaderObject, TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractDataArchivalHandlerProcessorForPost([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, BlittableJsonReaderObject configuration, string raftRequestId)
    {
        return RequestHandler.ServerStore.ModifyDatabaseDataArchival(context, RequestHandler.DatabaseName, configuration, raftRequestId);
    }
}
