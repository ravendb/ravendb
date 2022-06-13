using System;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.Documents.TcpHandlers;
using Sparrow.Collections;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Tcp;

internal abstract class AbstractTcpManagementHandlerProcessorForGetAll<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractTcpManagementHandlerProcessorForGetAll([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract ConcurrentSet<TcpConnectionOptions> GetRunningTcpConnections();

    public override async ValueTask ExecuteAsync()
    {
        var start = RequestHandler.GetStart();
        var pageSize = RequestHandler.GetPageSize();

        var minDuration = RequestHandler.GetLongQueryString("minSecDuration", false);
        var maxDuration = RequestHandler.GetLongQueryString("maxSecDuration", false);
        var ip = RequestHandler.GetStringQueryString("ip", false);
        var operationString = RequestHandler.GetStringQueryString("operation", false);

        TcpConnectionHeaderMessage.OperationTypes? operation = null;
        if (string.IsNullOrEmpty(operationString) == false)
            operation = (TcpConnectionHeaderMessage.OperationTypes)Enum.Parse(typeof(TcpConnectionHeaderMessage.OperationTypes), operationString, ignoreCase: true);

        using (ContextPool.AllocateOperationContext(out TOperationContext context))
        {
            var connections = GetRunningTcpConnections()
                .Where(connection => connection.CheckMatch(minDuration, maxDuration, ip, operation))
                .Skip(start)
                .Take(pageSize);

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WriteArray(context, "Results", connections, (w, c, connection) => c.Write(w, connection.GetConnectionStats()));

                writer.WriteEndObject();
            }
        }
    }
}
