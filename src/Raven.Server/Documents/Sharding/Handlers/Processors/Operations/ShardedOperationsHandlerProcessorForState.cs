using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Operations.Processors;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Operations;

internal class ShardedOperationsHandlerProcessorForState : AbstractOperationsHandlerProcessorForState<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedOperationsHandlerProcessorForState([NotNull] ShardedDatabaseRequestHandler requestHandler) 
        : base(requestHandler)
    {
    }

    protected override OperationState GetOperationState(long operationId) => RequestHandler.DatabaseContext.Operations.GetOperation(operationId)?.State;
}
