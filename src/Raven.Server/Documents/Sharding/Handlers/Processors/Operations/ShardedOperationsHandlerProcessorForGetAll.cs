using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Server.Documents.Operations;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Operations.Processors;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Operations;

internal class ShardedOperationsHandlerProcessorForGetAll : AbstractOperationsHandlerProcessorForGetAll<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedOperationsHandlerProcessorForGetAll([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override AbstractOperation GetOperation(long operationId) => RequestHandler.DatabaseContext.Operations.GetOperation(operationId);

    protected override IEnumerable<AbstractOperation> GetAllOperations() => RequestHandler.DatabaseContext.Operations.GetAll();
}
