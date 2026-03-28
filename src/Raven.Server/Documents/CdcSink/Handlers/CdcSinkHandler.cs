using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Server.Documents.CdcSink.Stats.Performance;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.CdcSink.Handlers;

public class CdcSinkHandler : DatabaseRequestHandler
{
    [RavenAction("/databases/*/admin/cdc-sink/test", "POST", AuthorizationStatus.DatabaseAdmin)]
    public async Task PostScriptTest()
    {
        using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var dbDoc = await context.ReadForMemoryAsync(RequestBodyStream(), "TestCdcSinkScript");
            var testScript = JsonDeserializationServer.TestCdcSinkScript(dbDoc);

            var result = CdcSinkProcess.TestScript(testScript, context, Database);

            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, result.ToJson());
            }
        }
    }

    [RavenAction("/databases/*/admin/cdc-sink/verify", "POST", AuthorizationStatus.DatabaseAdmin)]
    public async Task PostVerifySource()
    {
        using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var bodyJson = await context.ReadForMemoryAsync(RequestBodyStream(), "CdcSinkVerify");

            if (bodyJson.TryGet(nameof(CdcSinkVerifyRequest.ConnectionStringName), out string connectionStringName) == false ||
                string.IsNullOrEmpty(connectionStringName))
            {
                ThrowRequiredPropertyNameInRequest(nameof(CdcSinkVerifyRequest.ConnectionStringName));
            }

            var databaseRecord = Database.ReadDatabaseRecord();
            if (databaseRecord.SqlConnectionStrings.TryGetValue(connectionStringName, out var sqlConnectionString) == false)
            {
                var notFoundResult = new CdcSinkVerificationResult();
                notFoundResult.Errors.Add($"SQL connection string '{connectionStringName}' was not found in the database configuration.");

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, notFoundResult.ToJson());
                }
                return;
            }

            // Extract optional table names from request for table-level checks
            List<string> tableNames = null;
            if (bodyJson.TryGet(nameof(CdcSinkVerifyRequest.TableNames), out BlittableJsonReaderArray tablesArray) && tablesArray != null)
            {
                tableNames = new List<string>();
                foreach (var item in tablesArray)
                    tableNames.Add(item.ToString());
            }

            CdcSinkVerificationResult result;
            try
            {
                result = await CdcSinkSourceVerifier.VerifyAsync(sqlConnectionString, tableNames);
            }
            catch (Exception e)
            {
                result = new CdcSinkVerificationResult();
                result.Errors.Add($"Verification failed: {e}");
            }

            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, result.ToJson());
            }
        }
    }

    [RavenAction("/databases/*/cdc-sink/performance/live", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, SkipUsagesCount = true)]
    public async Task PerformanceLive()
    {
        using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
        {
            var sinks = GetProcessesToReportOn();

            var receiveBuffer = new ArraySegment<byte>(new byte[1024]);
            var receive = webSocket.ReceiveAsync(receiveBuffer, Database.DatabaseShutdown);

            await using (var ms = RecyclableMemoryStreamFactory.GetRecyclableStream())
            using (var collector = new LiveCdcSinkPerformanceCollector(Database, sinks))
            {
                await collector.SendStatsOrHeartbeatToWebSocket(receive, webSocket, ContextPool, ms, 100);

                while (Database.DatabaseShutdown.IsCancellationRequested == false)
                {
                    if (await collector.SendStatsOrHeartbeatToWebSocket(receive, webSocket, ContextPool, ms, 4000) == false)
                    {
                        break;
                    }
                }
            }
        }
    }

    private Dictionary<string, List<CdcSinkProcess>> GetProcessesToReportOn()
    {
        Dictionary<string, List<CdcSinkProcess>> sinks;
        var names = HttpContext.Request.Query["name"];

        if (names.Count == 0)
        {
            sinks = Database.CdcSinkLoader.Processes
                .GroupBy(x => x.Configuration.Name)
                .OrderBy(x => x.Key)
                .ToDictionary(x => x.Key, x => x.OrderBy(y => y.Table.Name).ToList());
        }
        else
        {
            sinks = Database.CdcSinkLoader.Processes
                .Where(x => names.Contains(x.Configuration.Name, StringComparer.OrdinalIgnoreCase) || names.Contains(x.Name, StringComparer.OrdinalIgnoreCase))
                .GroupBy(x => x.Configuration.Name)
                .OrderBy(x => x.Key)
                .ToDictionary(x => x.Key, x => x.OrderBy(y => y.Table.Name).ToList());
        }

        return sinks;
    }
}

public class CdcSinkVerifyRequest
{
    public string ConnectionStringName { get; set; }
    public List<string> TableNames { get; set; }
}
