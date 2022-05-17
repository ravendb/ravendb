using System.Collections.Concurrent;
using JetBrains.Annotations;
using Raven.Server.Documents.Changes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Changes;

internal class ChangesHandlerProcessorForGetConnectionsDebugInfo : AbstractChangesHandlerProcessorForGetConnectionsDebugInfo<DatabaseRequestHandler, DocumentsOperationContext, ChangesClientConnection>
{
    public ChangesHandlerProcessorForGetConnectionsDebugInfo([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override ConcurrentDictionary<long, ChangesClientConnection> GetConnections() => RequestHandler.Database.Changes.Connections;
}
