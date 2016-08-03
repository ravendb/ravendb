using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class TcpManagementHandler:DatabaseRequestHandler
    {
       
        [RavenAction("/databases/*/tcp/all", "GET", "/databases/{databaseName:string}/tcp/all?start={start:int}&pageSize={pageSize:int}")]
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
                    var skept = 0;
                    var taken = 0;

                    var isFirst = true;

                    writer.WriteStartArray();

                    foreach (var connection in connections)
                    {
                        
                        if (skept < start)
                        {
                            skept++;
                            continue;
                        }
                        
                        if (isFirst == false)
                            writer.WriteComma();

                        isFirst = false;

                        connection.GetConnectionStats(writer, context);

                        if (taken == take)
                            break;
                    }
                    writer.WriteEndArray();
                }
            }
            return Task.CompletedTask;
        }


        //Search for connections (duration, ip, type)
        [RavenAction("/databases/*/tcp/filter", "GET", "/databases/{databaseName:string}/tcp?minSecDuration={minSecDuration:long|optional}&maxSecDuration={maxSecDuration:long|optional}&ip={ip:string|optional}&operation={operation:long|optional}&pageSize={pageSize:int}&query={query:string}&start={start:int}&pageSize={pageSize:int}")]
        public Task FindConnection()
        {
            var start = GetStart();
            var take = GetPageSize(Database.Configuration.Core.MaxPageSize);
            var minDuration = GetLongQueryString("minSecDuration",false);
            var maxDuration = GetLongQueryString("maxSecDuration",false);
            var ip = GetStringQueryString("ip",false);
            var operationString = GetStringQueryString("operation",false);
            TcpConnectionHeaderMessage.OperationTypes? operation = null;

            if (string.IsNullOrEmpty(operationString) == false)
                operation =
                    (TcpConnectionHeaderMessage.OperationTypes)
                    Enum.Parse(typeof(TcpConnectionHeaderMessage.OperationTypes), operationString);

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var connections = Database.RunningTcpConnections;
                HttpContext.Response.StatusCode = 200;
                
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var skept = 0;
                    var taken = 0;

                    writer.WriteStartArray();
                    var isFirst = true;
                    foreach (var connection in connections)
                    {
                        if (connection.CheckMatch(minDuration, maxDuration, ip, operation) == false)
                            continue;
                        
                        if (skept < start)
                        {
                            skept++;
                            continue;
                        }

                        if (isFirst == false) 
                            writer.WriteComma();

                        connection.GetConnectionStats(writer, context);

                        isFirst = false;

                        if (taken == take)
                            break;

                        
                    }

                    writer.WriteEndArray();

                    
                }
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/tcp/shutdowngracefully", "GET", "/databases/{databaseName:string}/tcp/shutdowngracefully?id={id:long}")]
        public Task DropConnection()
        {
            var id = GetLongQueryString("id");

            var connections = Database.RunningTcpConnections;

            foreach (var tcpConnectionOptions in connections)
            {
                if (tcpConnectionOptions.Id == id)
                {
                    tcpConnectionOptions.Dispose();
                    break;
                }
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/tcp/detailedStats", "GET", "/databases/{databaseName:string}/tcp/detailedStats?id={id:long}")]
        public Task GetConnectionDetailedStats()
        {
            var id = GetLongQueryString("id");

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var connections = Database.RunningTcpConnections;
                HttpContext.Response.StatusCode = 200;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    foreach (var connection in connections)
                    {
                        if (connection.Id == id)
                        {
                            writer.WritePropertyName("BasicStats");

                            connection.GetConnectionStats(writer, context);
                            writer.WriteComma();
                            writer.WritePropertyName("OperationSpecificStats");
                            connection.GetTypeSpecificStats(writer,context);
                            break;
                        }
                    }

                    writer.WriteEndObject();


                }
            }
            return Task.CompletedTask;
        }




    }
}
