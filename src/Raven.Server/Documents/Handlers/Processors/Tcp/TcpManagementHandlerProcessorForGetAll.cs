using JetBrains.Annotations;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Collections;

namespace Raven.Server.Documents.Handlers.Processors.Tcp;

internal class TcpManagementHandlerProcessorForGetAll : AbstractTcpManagementHandlerProcessorForGetAll<DatabaseRequestHandler, DocumentsOperationContext>
{
    public TcpManagementHandlerProcessorForGetAll([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override ConcurrentSet<TcpConnectionOptions> GetRunningTcpConnections() => RequestHandler.Database.RunningTcpConnections;
}
