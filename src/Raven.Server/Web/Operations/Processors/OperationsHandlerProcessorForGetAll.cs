using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.Documents.Operations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Web.Operations.Processors;

internal class OperationsHandlerProcessorForGetAll : AbstractOperationsHandlerProcessorForGetAll<DatabaseRequestHandler, DocumentsOperationContext>
{
    public OperationsHandlerProcessorForGetAll([NotNull] DatabaseRequestHandler requestHandler) 
        : base(requestHandler)
    {
    }

    protected override AbstractOperation GetOperation(long operationId) => RequestHandler.Database.Operations.GetOperation(operationId);

    protected override IEnumerable<AbstractOperation> GetAllOperations() => RequestHandler.Database.Operations.GetAll();
}
