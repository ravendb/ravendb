using System.Linq;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.TcpHandlers;
using Sparrow.Collections;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Tcp;

internal abstract class AbstractTcpManagementHandlerProcessorForDelete<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractTcpManagementHandlerProcessorForDelete([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract ConcurrentSet<TcpConnectionOptions> GetRunningTcpConnections();

    public override async ValueTask ExecuteAsync()
    {
        var id = RequestHandler.GetLongQueryString("id");

        var connection = GetRunningTcpConnections()
            .FirstOrDefault(x => x.Id == id);

        if (connection == null)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            return;
        }

        // force a disconnection
        await connection.Stream.DisposeAsync();
        connection.TcpClient.Dispose();

        RequestHandler.NoContentStatus();
    }
}
