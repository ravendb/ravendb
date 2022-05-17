using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Changes;

internal class ChangesHandlerProcessorForDeleteConnections : AbstractChangesHandlerProcessorForDeleteConnections<DatabaseRequestHandler, DocumentsOperationContext>
{
    public ChangesHandlerProcessorForDeleteConnections([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override void Disconnect(long connectionId)
    {
        RequestHandler.Database.Changes.Disconnect(connectionId);
    }
}
