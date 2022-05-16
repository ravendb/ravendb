using System;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Web.Operations.Processors;

internal class OperationsHandlerProcessorForState : AbstractOperationsHandlerProcessorForState<DatabaseRequestHandler, DocumentsOperationContext>
{
    public OperationsHandlerProcessorForState([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override OperationState GetOperationState(long operationId) => RequestHandler.Database.Operations.GetOperation(operationId)?.State;
}
