using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Web.Operations.Processors;

internal class OperationsHandlerProcessorForGetNextOperationId : AbstractOperationsHandlerProcessorForGetNextOperationId<DatabaseRequestHandler, DocumentsOperationContext>
{
    public OperationsHandlerProcessorForGetNextOperationId([NotNull] DatabaseRequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    protected override long GetNextOperationId() => RequestHandler.Database.Operations.GetNextOperationId();
}
