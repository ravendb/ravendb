using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Server.Tcp;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class TcpManagementHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/tcp", "GET", "/databases/{databaseName:string}/tcp/all?start={start:int}&pageSize={pageSize:int}")]
        public Task GetAll()
        {
            var start = GetStart();
            var pageSize = GetPageSize();

            var minDuration = GetLongQueryString("minSecDuration", false);
            var maxDuration = GetLongQueryString("maxSecDuration", false);
            var ip = GetStringQueryString("ip", false);
            var operationString = GetStringQueryString("operation", false);

            TcpConnectionHeaderMessage.OperationTypes? operation = null;
            if (string.IsNullOrEmpty(operationString) == false)
                operation = (TcpConnectionHeaderMessage.OperationTypes)Enum.Parse(typeof(TcpConnectionHeaderMessage.OperationTypes), operationString, ignoreCase: true);

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var connections = Database.RunningTcpConnections
                    .Where(connection => connection.CheckMatch(minDuration, maxDuration, ip, operation))
                    .Skip(start)
                    .Take(pageSize);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WriteArray(context, "Results", connections, (w, c, connection) =>
                    {
                        c.Write(w, connection.GetConnectionStats(context));
                    });

                    writer.WriteEndObject();
                }
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/tcp", "DELETE", "/databases/{databaseName:string}/tcp/drop?id={id:long}")]
        public Task Delete()
        {
            var id = GetLongQueryString("id");

            var connection = Database.RunningTcpConnections
                .FirstOrDefault(x => x.Id == id);

            if (connection == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return Task.CompletedTask;
            }

            // force a disconnection
            connection.Stream.Dispose();
            connection.TcpClient.Dispose();

            return NoContent();
        }
    }
}