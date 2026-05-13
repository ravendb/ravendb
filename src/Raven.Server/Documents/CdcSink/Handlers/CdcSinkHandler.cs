using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.Documents.CdcSink.Schema;
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
            var request = JsonDeserializationServer.CdcSinkVerifyRequest(bodyJson);

            if (string.IsNullOrEmpty(request.ConnectionStringName))
            {
                ThrowRequiredPropertyNameInRequest(nameof(CdcSinkVerifyRequest.ConnectionStringName));
            }

            var databaseRecord = Database.ReadDatabaseRecord();
            if (databaseRecord.SqlConnectionStrings.TryGetValue(request.ConnectionStringName, out var sqlConnectionString) == false)
            {
                var notFoundResult = new CdcSinkVerificationResult();
                notFoundResult.Errors.Add($"SQL connection string '{request.ConnectionStringName}' was not found in the database configuration.");

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, notFoundResult.ToJson());
                }
                return;
            }

            CdcSinkVerificationResult result;
            try
            {
                result = await CdcSinkSourceVerifier.VerifyAsync(sqlConnectionString, request.TableNames);
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

    [RavenAction("/databases/*/admin/cdc-sink/schema", "POST", AuthorizationStatus.DatabaseAdmin)]
    public async Task PostSchema()
    {
        using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var bodyJson = await context.ReadForMemoryAsync(RequestBodyStream(), "CdcSinkSchemaRequest");
            var request = JsonDeserializationServer.CdcSinkSchemaRequest(bodyJson);

            var connection = ResolveConnection(request);

            CdcSinkSchemaDiscovery discovery;
            try
            {
                discovery = CdcSinkSchemaDiscovery.For(connection.FactoryName);
            }
            catch (InvalidOperationException e)
            {
                throw new InvalidOperationException($"Cannot discover CDC schema: {e.Message}", e);
            }

            var schema = await discovery.DiscoverAsync(connection.ConnectionString, request.Schemas, Database.DatabaseShutdown);

            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, schema.ToJson());
            }
        }
    }

    /// <summary>
    /// Inline <see cref="CdcSinkSchemaRequest.Connection"/> takes precedence — Studio's Task
    /// Creation view sends raw credentials because the connection-string named record may
    /// not exist in <c>databaseRecord.SqlConnectionStrings</c> yet. Falls back to the named
    /// lookup for post-save callers.
    /// </summary>
    private SqlConnectionString ResolveConnection(CdcSinkSchemaRequest request)
    {
        if (request.Connection != null && string.IsNullOrEmpty(request.Connection.FactoryName) == false && string.IsNullOrEmpty(request.Connection.ConnectionString) == false)
            return request.Connection;

        if (string.IsNullOrEmpty(request.ConnectionStringName))
            throw new InvalidOperationException(
                $"Provide either '{nameof(CdcSinkSchemaRequest.Connection)}' (inline {nameof(SqlConnectionString.FactoryName)} + {nameof(SqlConnectionString.ConnectionString)}) " +
                $"or '{nameof(CdcSinkSchemaRequest.ConnectionStringName)}'.");

        var databaseRecord = Database.ReadDatabaseRecord();
        if (databaseRecord.SqlConnectionStrings.TryGetValue(request.ConnectionStringName, out var named) == false)
            throw new InvalidOperationException($"SQL connection string '{request.ConnectionStringName}' was not found in the database configuration.");

        return named;
    }

    [RavenAction("/databases/*/cdc-sink/performance", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task Performance()
    {
        var sinks = GetProcessesToReportOn();

        var stats = new List<CdcSinkTaskPerformanceStats>(sinks.Count);
        foreach (var kvp in sinks)
        {
            var processPerformanceStats = new List<CdcSinkProcessPerformanceStats>(kvp.Value.Count);
            foreach (var process in kvp.Value)
            {
                processPerformanceStats.Add(new CdcSinkProcessPerformanceStats
                {
                    Performance = process.GetPerformanceStats()
                });
            }

            stats.Add(new CdcSinkTaskPerformanceStats
            {
                TaskId = kvp.Value[0].TaskId,
                TaskName = kvp.Key,
                Stats = processPerformanceStats.ToArray()
            });
        }

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
        {
            writer.WriteCdcSinkTaskPerformanceStats(context, stats);
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

    private SortedDictionary<string, List<CdcSinkProcess>> GetProcessesToReportOn()
    {
        var names = HttpContext.Request.Query["name"];
        var sinks = new SortedDictionary<string, List<CdcSinkProcess>>(StringComparer.OrdinalIgnoreCase);

        foreach (var process in Database.CdcSinkLoader.Processes)
        {
            if (names.Count > 0 &&
                names.Contains(process.Configuration.Name, StringComparer.OrdinalIgnoreCase) == false &&
                names.Contains(process.Name, StringComparer.OrdinalIgnoreCase) == false)
            {
                continue;
            }

            if (sinks.TryGetValue(process.Configuration.Name, out var list) == false)
            {
                list = new List<CdcSinkProcess>();
                sinks[process.Configuration.Name] = list;
            }

            list.Add(process);
        }

        foreach (var list in sinks.Values)
        {
            list.Sort((x, y) => string.Compare(x.Name, y.Name, StringComparison.Ordinal));
        }

        return sinks;
    }
}

public class CdcSinkVerifyRequest
{
    public string ConnectionStringName { get; set; }
    public List<string> TableNames { get; set; }
}

public class CdcSinkSchemaRequest
{
    /// <summary>
    /// Inline credentials. Required path for Studio's Task Creation view, where the user
    /// is editing the connection but hasn't saved it to <c>databaseRecord.SqlConnectionStrings</c> yet.
    /// </summary>
    public SqlConnectionString Connection { get; set; }

    /// <summary>
    /// Optional fallback for post-save callers. Ignored when <see cref="Connection"/> is populated.
    /// </summary>
    public string ConnectionStringName { get; set; }

    /// <summary>
    /// Provider-specific schema filter. Currently only consumed by PostgreSQL (defaults to
    /// <c>["public"]</c> when null/empty).
    /// </summary>
    public string[] Schemas { get; set; }
}
