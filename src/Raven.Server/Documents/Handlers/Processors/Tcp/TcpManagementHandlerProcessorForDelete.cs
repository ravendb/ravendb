using JetBrains.Annotations;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Collections;

namespace Raven.Server.Documents.Handlers.Processors.Tcp;

internal class TcpManagementHandlerProcessorForDelete : AbstractTcpManagementHandlerProcessorForDelete<DatabaseRequestHandler, DocumentsOperationContext>
{
    public TcpManagementHandlerProcessorForDelete([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override ConcurrentSet<TcpConnectionOptions> GetRunningTcpConnections() => RequestHandler.Database.RunningTcpConnections;
}
