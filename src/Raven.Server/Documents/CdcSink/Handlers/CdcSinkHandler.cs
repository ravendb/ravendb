using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
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
using System.Text.RegularExpressions;
using Raven.Server.SqlMigration;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.CdcSink.Handlers;

public class CdcSinkHandler : DatabaseRequestHandler
{
    /// <summary>
    /// Hard cap on the number of rows the test-mapping endpoint will fetch + materialise + run
    /// scripts against in a single request. The endpoint buffers the full result set in memory
    /// and writes it as one JSON response, so this cap also bounds the worst-case response size.
    /// </summary>
    internal const int MaxAllowedTestRows = 5000;

    /// <summary>
    /// Identifier shape gate. Both endpoints interpolate user-supplied identifiers into raw
    /// SQL (the migrator's <c>QuoteTable</c> / <c>QuoteColumn</c> for table and column names
    /// in the test endpoint, and Postgres' <c>INFORMATION_SCHEMA</c> filter for schema names
    /// in the schema-discovery endpoint). The proper provider-side quoting fix is tracked on
    /// RavenDB-26636 (Postgres returns raw identifiers; SQL Server and MySQL escape brackets
    /// / backticks incorrectly); until then, reject anything outside the standard SQL
    /// identifier shape so a typo or malicious value can't break out of the quoted context.
    /// </summary>
    private static readonly Regex IdentifierPattern = new("^[A-Za-z_][A-Za-z0-9_]*$", RegexOptions.Compiled);

    private static bool TryValidateIdentifier(string value, string fieldName, CdcSinkSourceSchema schemaResult, TestCdcSinkMappingResult testResult, bool allowEmpty = true)
    {
        // Schema fields can legitimately be empty (default-schema fallback handles it). For
        // table / PK / column identifiers, empty would flow into ORDER BY / WHERE generation
        // as a SQL syntax error — callers in those positions pass allowEmpty: false.
        if (string.IsNullOrEmpty(value))
        {
            if (allowEmpty)
                return true;
            var emptyError = $"'{fieldName}' must not be empty.";
            if (schemaResult != null)
                schemaResult.Errors.Add(emptyError);
            if (testResult != null)
                testResult.Errors.Add(emptyError);
            return false;
        }
        if (IdentifierPattern.IsMatch(value))
            return true;
        var error = $"'{fieldName}' value '{value}' contains invalid characters. Use letters, digits, and underscores only.";
        if (schemaResult != null)
            schemaResult.Errors.Add(error);
        if (testResult != null)
            testResult.Errors.Add(error);
        return false;
    }

    [RavenAction("/databases/*/admin/cdc-sink/test", "POST", AuthorizationStatus.DatabaseAdmin)]
    public async Task PostScriptTest()
    {
        // Link the per-request abort with the server shutdown so a client that closes the
        // connection mid-call cancels the upstream FetchRowsAsync — the source-DB query can
        // otherwise hang the request for minutes on a slow remote driver.
        using (var cts = CancellationTokenSource.CreateLinkedTokenSource(Database.DatabaseShutdown, HttpContext.RequestAborted))
        using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var bodyJson = await context.ReadForMemoryAsync(RequestBodyStream(), "TestCdcSinkMappingRequest");
            var request = JsonDeserializationClient.TestCdcSinkMappingRequest(bodyJson);

            var result = await ExecuteTestMappingAsync(context, request, cts.Token);

            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, result.ToJson());
            }
        }
    }

    private async Task<TestCdcSinkMappingResult> ExecuteTestMappingAsync(DocumentsOperationContext context, TestCdcSinkMappingRequest request, CancellationToken ct)
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

        // Reject any user-supplied identifier that doesn't match the standard SQL shape — these
        // flow into raw SQL via the provider's QuoteTable / QuoteColumn (see RavenDB-26636 for the
        // deeper provider-side fix). Same gate applied to the schema-discovery endpoint below.
        // SourceTableSchema can be empty (default-schema fallback handles it); the resolved
        // targetSchema is validated separately below. SourceTableName must not be empty.
        if (TryValidateIdentifier(request.SourceTableSchema, nameof(TestCdcSinkMappingRequest.SourceTableSchema), schemaResult: null, testResult: result) == false ||
            TryValidateIdentifier(request.SourceTableName, nameof(TestCdcSinkMappingRequest.SourceTableName), schemaResult: null, testResult: result, allowEmpty: false) == false)
            return result;

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

        // DatabaseDriverDispatcher accepts a wider provider set than the CDC sink (it is shared
        // with the SQL Migration feature, which supports Oracle). Mirror the gate that the schema
        // and verify endpoints already apply so the test endpoint can't silently preview against a
        // provider the rest of the CDC sink would reject.
        if (CdcSinkSchemaDiscovery.IsSupportedFactoryName(connection.FactoryName) == false)
        {
            result.Errors.Add(CdcSinkSchemaDiscovery.UnsupportedProviderMessage(connection.FactoryName));
            return result;
        }

        // CDC runtime substitutes a provider-default schema ("public" / "dbo" / DB name) when a
        // saved config left SourceTableSchema empty. Studio's /admin/cdc-sink/schema response
        // always carries explicit schemas, so the request side may pass "public" against a config
        // that left the field empty — or vice versa. Resolve the default once and apply it to
        // both sides of the comparison + the runner so the test endpoint mirrors what the
        // runtime would do.
        // MySQL's ResolveDefaultSchema parses the user-supplied connection string via
        // MySqlConnectionStringBuilder, which throws on malformed input. Without this catch
        // the failure escaped ExecuteTestMappingAsync as HTTP 500 — inconsistent with every
        // other failure on the endpoint. Mirror the row-fetch / schema-discovery catch shape:
        // structured Errors with the full driver exception detail, Logger.Warn for the stack.
        string defaultSchema;
        try
        {
            defaultSchema = CdcSinkSchemaDiscovery.ResolveDefaultSchema(connection.FactoryName, connection.ConnectionString);
        }
        catch (Exception e)
        {
            result.Errors.Add("Failed to resolve provider default schema from the connection string: " + e);
            if (Logger.IsWarnEnabled)
                Logger.Warn("CDC test-mapping default-schema resolution failed", e);
            return result;
        }
        var targetSchema = string.IsNullOrEmpty(request.SourceTableSchema) ? defaultSchema : request.SourceTableSchema;

        // Validate the resolved value, not just the raw request field. For MySQL the default
        // schema is pulled from the connection string's Database key (user-controlled) — a
        // malformed value would bypass the request/config-side gates and reach QuoteTable. The
        // resolved targetSchema is what actually flows into raw SQL, so the gate belongs here.
        if (TryValidateIdentifier(targetSchema, "targetSchema", schemaResult: null, testResult: result, allowEmpty: false) == false)
            return result;

        var matches = request.Configuration.Tables?
            .Where(t =>
                string.Equals(
                    string.IsNullOrEmpty(t.SourceTableSchema) ? defaultSchema : t.SourceTableSchema,
                    targetSchema,
                    StringComparison.OrdinalIgnoreCase) &&
                string.Equals(t.SourceTableName, request.SourceTableName, StringComparison.OrdinalIgnoreCase))
            .ToList();

        if (matches == null || matches.Count == 0)
        {
            result.Errors.Add($"Configuration has no table matching '{targetSchema}.{request.SourceTableName}'.");
            return result;
        }

        if (matches.Count > 1)
        {
            // Two same-name-different-case entries in the config (e.g. "Customers" and "customers").
            // FirstOrDefault would silently pick one; surface the ambiguity instead so Studio can
            // flag the duplicate before the user saves the CDC task.
            result.Errors.Add($"Configuration has {matches.Count} tables matching '{targetSchema}.{request.SourceTableName}' " +
                              "(case-insensitive). Remove the duplicates from Tables[] before testing.");
            return result;
        }

        var targetTable = matches[0];

        // Same identifier gate applied to the per-table column names that flow through the
        // row-fetch query builder (BuildSelectFirstRowsQuery for ORDER BY, BuildSelectByPrimaryKeyQuery
        // for the WHERE clause). SourceTableSchema can be empty (default-schema fallback); the
        // resolved targetSchema is validated separately above. Table / PK / column names
        // generate raw SQL when interpolated so empty is forbidden for them.
        if (TryValidateIdentifier(targetTable.SourceTableSchema, $"{nameof(CdcSinkTableConfig)}.{nameof(CdcSinkTableConfig.SourceTableSchema)}", schemaResult: null, testResult: result) == false ||
            TryValidateIdentifier(targetTable.SourceTableName, $"{nameof(CdcSinkTableConfig)}.{nameof(CdcSinkTableConfig.SourceTableName)}", schemaResult: null, testResult: result, allowEmpty: false) == false)
            return result;
        if (targetTable.PrimaryKeyColumns != null)
        {
            foreach (var pkColumn in targetTable.PrimaryKeyColumns)
            {
                if (TryValidateIdentifier(pkColumn, $"{nameof(CdcSinkTableConfig)}.{nameof(CdcSinkTableConfig.PrimaryKeyColumns)}", schemaResult: null, testResult: result, allowEmpty: false) == false)
                    return result;
            }
        }
        if (targetTable.Columns != null)
        {
            foreach (var column in targetTable.Columns)
            {
                if (TryValidateIdentifier(column.Column, $"{nameof(CdcColumnMapping)}.{nameof(CdcColumnMapping.Column)}", schemaResult: null, testResult: result, allowEmpty: false) == false)
                    return result;
            }
        }

        IDatabaseDriver driver;
        try
        {
            // Pass the resolved targetSchema into the driver so the Postgres migrator's
            // FindSchema() can locate non-default-schema tables when ByPrimaryKey mode looks
            // up PK column types for ValueAsObject coercion. NpgSqlSchemaQueries otherwise
            // folds null to ["public"] in its INFORMATION_SCHEMA filter and rejects anything
            // outside that schema as "table not found". SQL Server and MySQL drivers ignore
            // the schemas argument, so this is a Postgres-only behaviour change.
            driver = DatabaseDriverDispatcher.CreateDriver(
                connection.FactoryName,
                connection.ConnectionString,
                schemas: new[] { targetSchema });
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
                tableSchema: string.IsNullOrEmpty(targetTable.SourceTableSchema) ? defaultSchema : targetTable.SourceTableSchema,
                tableName: targetTable.SourceTableName,
                primaryKeyColumns: targetTable.PrimaryKeyColumns,
                mode: request.RowSelector == TestCdcSinkRowSelector.First ? RowFetchMode.First : RowFetchMode.ByPrimaryKey,
                primaryKeyValues: request.PrimaryKeyValues,
                maxRows: request.MaxRows,
                ct: ct);
        }
        catch (Exception e)
        {
            // The admin caller is interested in the full driver message — host, port, internal
            // error code — so they can diagnose the source-side problem directly. Log a warning
            // for the stack trace and include the message text in the structured response.
            result.Errors.Add("Failed to fetch rows from the source database: " + e);
            if (Logger.IsWarnEnabled)
                Logger.Warn("CDC test-mapping row fetch failed", e);
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
            fetched.ColumnNames, fetched.Rows, request.Operation, defaultSchema);
    }

    private SqlConnectionString ResolveTestConnection(TestCdcSinkMappingRequest request)
        => ResolveSqlConnection(
            request.Connection,
            request.Configuration?.ConnectionStringName,
            inlineFieldName: nameof(TestCdcSinkMappingRequest.Connection),
            namedFieldName: nameof(CdcSinkConfiguration.ConnectionStringName));

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
        // Same per-request cancellation as PostScriptTest — a slow remote discovery should
        // not survive the client closing the HTTP connection.
        using (var cts = CancellationTokenSource.CreateLinkedTokenSource(Database.DatabaseShutdown, HttpContext.RequestAborted))
        using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var bodyJson = await context.ReadForMemoryAsync(RequestBodyStream(), "CdcSinkSchemaRequest");
            var request = JsonDeserializationClient.CdcSinkSchemaRequest(bodyJson);

            var result = await ExecuteSchemaDiscoveryAsync(request, cts.Token);

            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, result.ToJson());
            }
        }
    }

    private async Task<CdcSinkSourceSchema> ExecuteSchemaDiscoveryAsync(CdcSinkSchemaRequest request, CancellationToken ct)
    {
        var result = new CdcSinkSourceSchema();

        if (request.Schemas != null)
        {
            foreach (var schemaName in request.Schemas)
            {
                if (string.IsNullOrEmpty(schemaName))
                {
                    result.Errors.Add($"'{nameof(CdcSinkSchemaRequest.Schemas)}' must not contain empty entries.");
                    return result;
                }
                if (TryValidateIdentifier(schemaName, $"{nameof(CdcSinkSchemaRequest.Schemas)}[]", schemaResult: result, testResult: null) == false)
                    return result;
            }
        }

        SqlConnectionString connection;
        CdcSinkSchemaDiscovery discovery;
        try
        {
            connection = ResolveConnection(request);
            discovery = CdcSinkSchemaDiscovery.For(connection.FactoryName);
        }
        catch (InvalidOperationException e)
        {
            result.Errors.Add(e.Message);
            return result;
        }

        try
        {
            return await discovery.DiscoverAsync(connection.ConnectionString, request.Schemas, ct);
        }
        catch (Exception e)
        {
            // Surface the driver's full message (host / port / internal code) to the admin
            // caller — they need the details to diagnose source-side problems. Logger.Warn for
            // the stack trace.
            result.Errors.Add("Schema discovery against the source database failed: " + e);
            if (Logger.IsWarnEnabled)
                Logger.Warn("CDC schema discovery failed", e);
            return result;
        }
    }

    /// <summary>
    /// Inline <see cref="CdcSinkSchemaRequest.Connection"/> takes precedence — Studio's Task
    /// Creation view sends raw credentials because the connection-string named record may
    /// not exist in <c>databaseRecord.SqlConnectionStrings</c> yet. Falls back to the named
    /// lookup for post-save callers.
    /// </summary>
    private SqlConnectionString ResolveConnection(CdcSinkSchemaRequest request)
        => ResolveSqlConnection(
            request.Connection,
            request.ConnectionStringName,
            inlineFieldName: nameof(CdcSinkSchemaRequest.Connection),
            namedFieldName: nameof(CdcSinkSchemaRequest.ConnectionStringName));

    /// <summary>
    /// Common inline-vs-named connection-string resolver for the CDC admin endpoints.
    /// Inline <paramref name="inline"/> (a fully-populated <see cref="SqlConnectionString"/>)
    /// wins when present; otherwise <paramref name="connectionStringName"/> is looked up in
    /// <c>databaseRecord.SqlConnectionStrings</c>. The two field-name parameters only affect
    /// the error-message text — each endpoint shows the property name its own request DTO uses.
    /// </summary>
    private SqlConnectionString ResolveSqlConnection(
        SqlConnectionString inline,
        string connectionStringName,
        string inlineFieldName,
        string namedFieldName)
    {
        if (inline != null
            && string.IsNullOrEmpty(inline.FactoryName) == false
            && string.IsNullOrEmpty(inline.ConnectionString) == false)
        {
            return inline;
        }

        if (string.IsNullOrEmpty(connectionStringName))
            throw new InvalidOperationException(
                $"Provide either '{inlineFieldName}' (inline {nameof(SqlConnectionString.FactoryName)} + {nameof(SqlConnectionString.ConnectionString)}) " +
                $"or '{namedFieldName}'.");

        var databaseRecord = Database.ReadDatabaseRecord();
        if (databaseRecord.SqlConnectionStrings.TryGetValue(connectionStringName, out var named) == false)
            throw new InvalidOperationException($"SQL connection string '{connectionStringName}' was not found in the database configuration.");

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

