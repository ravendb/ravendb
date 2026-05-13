using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Client.Documents.Operations.CdcSink.Test;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.Documents.CdcSink.Schema;
using Raven.Server.Documents.CdcSink.Stats.Performance;
using Raven.Server.Documents.CdcSink.Test;
using Raven.Client.Json.Serialization;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.SqlMigration;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.CdcSink.Handlers;

public class CdcSinkHandler : DatabaseRequestHandler
{
    /// <summary>
    /// Hard cap on the number of rows the test-mapping endpoint will fetch + materialise + run
    /// scripts against in a single request. A previewing tool (Studio's "Test" button) only
    /// needs a handful; larger samples will eventually need a streaming response — see the
    /// §Risks block in the original plan for the streaming follow-up.
    /// </summary>
    internal const int MaxAllowedTestRows = 5000;

    [RavenAction("/databases/*/admin/cdc-sink/test", "POST", AuthorizationStatus.DatabaseAdmin)]
    public async Task PostScriptTest()
    {
        using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var bodyJson = await context.ReadForMemoryAsync(RequestBodyStream(), "TestCdcSinkMappingRequest");
            var request = JsonDeserializationClient.TestCdcSinkMappingRequest(bodyJson);

            var result = await ExecuteTestMappingAsync(context, request);

            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, result.ToJson());
            }
        }
    }

    private async Task<TestCdcSinkMappingResult> ExecuteTestMappingAsync(DocumentsOperationContext context, TestCdcSinkMappingRequest request)
    {
        var result = new TestCdcSinkMappingResult();

        if (request.Configuration == null)
        {
            result.Errors.Add($"'{nameof(TestCdcSinkMappingRequest.Configuration)}' is required.");
            return result;
        }
        if (string.IsNullOrEmpty(request.SourceTableName))
        {
            result.Errors.Add($"'{nameof(TestCdcSinkMappingRequest.SourceTableName)}' is required.");
            return result;
        }
        if (request.MaxRows < 1 || request.MaxRows > MaxAllowedTestRows)
        {
            result.Errors.Add($"'{nameof(TestCdcSinkMappingRequest.MaxRows)}' must be between 1 and {MaxAllowedTestRows:N0}.");
            return result;
        }
        if (request.RowSelector == TestCdcSinkRowSelector.ByPrimaryKey && request.MaxRows > 1)
        {
            result.Errors.Add($"'{nameof(TestCdcSinkMappingRequest.MaxRows)}' must be 1 when '{nameof(TestCdcSinkMappingRequest.RowSelector)}' is '{nameof(TestCdcSinkRowSelector.ByPrimaryKey)}'.");
            return result;
        }
        if (request.RowSelector == TestCdcSinkRowSelector.ByPrimaryKey && (request.PrimaryKeyValues == null || request.PrimaryKeyValues.Length == 0))
        {
            result.Errors.Add($"'{nameof(TestCdcSinkMappingRequest.PrimaryKeyValues)}' is required when '{nameof(TestCdcSinkMappingRequest.RowSelector)}' is '{nameof(TestCdcSinkRowSelector.ByPrimaryKey)}'.");
            return result;
        }

        SqlConnectionString connection;
        try
        {
            connection = ResolveTestConnection(request);
        }
        catch (InvalidOperationException e)
        {
            result.Errors.Add(e.Message);
            return result;
        }

        var targetSchema = request.SourceTableSchema ?? string.Empty;
        var targetTable = request.Configuration.Tables?.FirstOrDefault(t =>
            string.Equals(t.SourceTableSchema ?? string.Empty, targetSchema, StringComparison.OrdinalIgnoreCase) &&
            string.Equals(t.SourceTableName, request.SourceTableName, StringComparison.OrdinalIgnoreCase));
        if (targetTable == null)
        {
            result.Errors.Add($"Configuration has no table matching '{targetSchema}.{request.SourceTableName}'.");
            return result;
        }

        IDatabaseDriver driver;
        try
        {
            driver = DatabaseDriverDispatcher.CreateDriver(connection.FactoryName, connection.ConnectionString);
        }
        catch (InvalidOperationException e)
        {
            result.Errors.Add(e.Message);
            return result;
        }

        MigratorRowFetchResult fetched;
        try
        {
            fetched = await driver.FetchRowsAsync(
                tableSchema: targetTable.SourceTableSchema,
                tableName: targetTable.SourceTableName,
                primaryKeyColumns: targetTable.PrimaryKeyColumns,
                mode: request.RowSelector == TestCdcSinkRowSelector.First ? RowFetchMode.First : RowFetchMode.ByPrimaryKey,
                primaryKeyValues: request.PrimaryKeyValues,
                maxRows: request.MaxRows,
                ct: Database.DatabaseShutdown);
        }
        catch (Exception e)
        {
            result.Errors.Add($"Failed to fetch rows from source: {e.Message}");
            return result;
        }

        if (fetched.Rows.Count == 0)
        {
            result.Errors.Add(request.RowSelector == TestCdcSinkRowSelector.ByPrimaryKey
                ? "No row found in the source table for the supplied primary-key values."
                : "Source table is empty.");
            return result;
        }

        return CdcSinkTestRunner.Run(
            Database, context, request.Configuration, targetTable,
            fetched.ColumnNames, fetched.Rows, request.Operation);
    }

    private SqlConnectionString ResolveTestConnection(TestCdcSinkMappingRequest request)
    {
        if (request.Connection != null
            && string.IsNullOrEmpty(request.Connection.FactoryName) == false
            && string.IsNullOrEmpty(request.Connection.ConnectionString) == false)
        {
            return request.Connection;
        }

        var name = request.Configuration?.ConnectionStringName;
        if (string.IsNullOrEmpty(name))
            throw new InvalidOperationException(
                $"Provide either '{nameof(TestCdcSinkMappingRequest.Connection)}' (inline {nameof(SqlConnectionString.FactoryName)} + {nameof(SqlConnectionString.ConnectionString)}) " +
                $"or '{nameof(CdcSinkConfiguration.ConnectionStringName)}' on the configuration.");

        var databaseRecord = Database.ReadDatabaseRecord();
        if (databaseRecord.SqlConnectionStrings.TryGetValue(name, out var named) == false)
            throw new InvalidOperationException($"SQL connection string '{name}' was not found in the database configuration.");

        return named;
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
            var request = JsonDeserializationClient.CdcSinkSchemaRequest(bodyJson);

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

