using System.Net.WebSockets;
using JetBrains.Annotations;
using Raven.Server.Documents.Changes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Changes;

internal class ChangesHandlerProcessorForGetChanges : AbstractChangesHandlerProcessorForGetChanges<DatabaseRequestHandler, DocumentsOperationContext, ChangesClientConnection>
{
    public ChangesHandlerProcessorForGetChanges([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override ChangesClientConnection CreateChangesClientConnection(WebSocket webSocket, bool fromStudio) => new(webSocket, RequestHandler.Database, fromStudio);

    protected override void Connect(ChangesClientConnection connection)
    {
        RequestHandler.Database.Changes.Connect(connection);
    }

    protected override void Disconnect(ChangesClientConnection connection)
    {
        RequestHandler.Database.Changes.Disconnect(connection.Id);
    }
}
