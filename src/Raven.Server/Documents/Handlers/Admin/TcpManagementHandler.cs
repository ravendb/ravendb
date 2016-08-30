using System;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class TcpManagementHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/tcp/all", "GET",
            "/databases/{databaseName:string}/tcp/all?start={start:int}&pageSize={pageSize:int}")]
        public Task GetAllConnections()
        {
            var start = GetStart();
            var take = GetPageSize(Database.Configuration.Core.MaxPageSize);
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var connections = Database.RunningTcpConnections;
                HttpContext.Response.StatusCode = 200;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var isFirst = true;

                    writer.WriteStartArray();

                    foreach (var connection in connections)
                    {
                        if (start > 0)
                        {
                            start--;
                            continue;
                        }
                        if (--take <= 0)
                            break;

                        if (isFirst == false)
                            writer.WriteComma();

                        isFirst = false;

                        context.Write(writer, connection.GetConnectionStats(context));
                    }
                    writer.WriteEndArray();
                }
            }
            return Task.CompletedTask;
        }


        //Search for connections (duration, ip, type)
        [RavenAction("/databases/*/tcp/filter", "GET",
            "/databases/{databaseName:string}/tcp?minSecDuration={minSecDuration:long|optional}&maxSecDuration={maxSecDuration:long|optional}&ip={ip:string|optional}&operation={operation:long|optional}&pageSize={pageSize:int}&query={query:string}&start={start:int}&pageSize={pageSize:int}"
            )]
        public Task FindConnection()
        {
            var start = GetStart();
            var take = GetPageSize(Database.Configuration.Core.MaxPageSize);
            var minDuration = GetLongQueryString("minSecDuration", false);
            var maxDuration = GetLongQueryString("maxSecDuration", false);
            var ip = GetStringQueryString("ip", false);
            var operationString = GetStringQueryString("operation", false);
            TcpConnectionHeaderMessage.OperationTypes? operation = null;

            if (string.IsNullOrEmpty(operationString) == false)
                operation =
                    (TcpConnectionHeaderMessage.OperationTypes)
                        Enum.Parse(typeof (TcpConnectionHeaderMessage.OperationTypes), operationString);

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var connections = Database.RunningTcpConnections;
                HttpContext.Response.StatusCode = 200;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartArray();
                    var isFirst = true;
                    foreach (var connection in connections)
                    {
                        if (connection.CheckMatch(minDuration, maxDuration, ip, operation) == false)
                            continue;

                        if (start > 0)
                        {
                            start--;
                            continue;
                        }

                        if (--take <= 0)
                            break;

                        if (isFirst == false)
                            writer.WriteComma();

                        context.Write(writer, connection.GetConnectionStats(context));

                        isFirst = false;
                    }

                    writer.WriteEndArray();
                }
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/tcp/drop", "GET",
            "/databases/{databaseName:string}/tcp/drop?id={id:long}")]
        public Task DropConnection()
        {
            var id = GetLongQueryString("id");

            var connections = Database.RunningTcpConnections;

            foreach (var tcpConnectionOptions in connections)
            {
                if (tcpConnectionOptions.Id == id)
                {
                    tcpConnectionOptions.Dispose();
                    return Task.CompletedTask;

                }
            }
            HttpContext.Response.StatusCode = (int) HttpStatusCode.NotFound;
            return Task.CompletedTask;
        }
    }
}