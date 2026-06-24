using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Test;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.CdcSink.Schema;
using Raven.Server.Documents.CdcSink.Test;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.SqlMigration;
using Sparrow.Json;

namespace Raven.Server.Documents.CdcSink.Handlers.Processors;

internal sealed class CdcSinkHandlerProcessorForTest : AbstractCdcSinkHandlerProcessorForTest<DatabaseRequestHandler, DocumentsOperationContext>
{
    public CdcSinkHandlerProcessorForTest([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        // Link the per-request abort with the server shutdown so a client that closes the
        // connection mid-call cancels the upstream FetchRowsAsync - the source-DB query can
        // otherwise hang the request for minutes on a slow remote driver.
        using (var cts = CancellationTokenSource.CreateLinkedTokenSource(RequestHandler.Database.DatabaseShutdown, HttpContext.RequestAborted))
        using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var bodyJson = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "TestCdcSinkMappingRequest");
            var request = JsonDeserializationClient.TestCdcSinkMappingRequest(bodyJson);

            var result = await ExecuteTestMappingAsync(context, request, cts.Token);

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
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
        if (request.MaxRows < 1 || request.MaxRows > CdcSinkRequestValidation.MaxAllowedTestRows)
        {
            result.Errors.Add($"'{nameof(TestCdcSinkMappingRequest.MaxRows)}' must be between 1 and {CdcSinkRequestValidation.MaxAllowedTestRows:N0}.");
            return result;
        }
        // C# enums are int-backed and accept arbitrary numeric values from the JSON body. Reject
        // out-of-range values here so downstream branches (PK validation block, row-fetch mode
        // ternary, runner's Delete/Upsert branch) only see defined cases.
        if (Enum.IsDefined(typeof(TestCdcSinkRowSelector), request.RowSelector) == false)
        {
            result.Errors.Add($"'{nameof(TestCdcSinkMappingRequest.RowSelector)}' value '{(int)request.RowSelector}' is not a valid {nameof(TestCdcSinkRowSelector)}. Allowed values: {string.Join(", ", Enum.GetNames<TestCdcSinkRowSelector>())}.");
            return result;
        }
        if (Enum.IsDefined(typeof(TestCdcSinkOperation), request.Operation) == false)
        {
            result.Errors.Add($"'{nameof(TestCdcSinkMappingRequest.Operation)}' value '{(int)request.Operation}' is not a valid {nameof(TestCdcSinkOperation)}. Allowed values: {string.Join(", ", Enum.GetNames<TestCdcSinkOperation>())}.");
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

        // Reject any user-supplied identifier that doesn't match the standard SQL shape - these
        // flow into raw SQL via the provider's QuoteTable / QuoteColumn. Same gate applied to the
        // schema-discovery endpoint below.
        // SourceTableSchema can be empty (default-schema fallback handles it); the resolved
        // targetSchema is validated separately below. SourceTableName must not be empty.
        if (CdcSinkRequestValidation.TryValidateIdentifier(request.SourceTableSchema, nameof(TestCdcSinkMappingRequest.SourceTableSchema), schemaResult: null, testResult: result) == false ||
            CdcSinkRequestValidation.TryValidateIdentifier(request.SourceTableName, nameof(TestCdcSinkMappingRequest.SourceTableName), schemaResult: null, testResult: result, allowEmpty: false) == false)
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
        // that left the field empty - or vice versa. Resolve the default once and apply it to
        // both sides of the comparison + the runner so the test endpoint mirrors what the
        // runtime would do.
        // MySQL's ResolveDefaultSchema parses the user-supplied connection string via
        // MySqlConnectionStringBuilder, which throws on malformed input. Catch it and surface
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
        // schema is pulled from the connection string's Database key (user-controlled) - a
        // malformed value would bypass the request/config-side gates and reach QuoteTable. The
        // resolved targetSchema is what actually flows into raw SQL, so the gate belongs here.
        if (CdcSinkRequestValidation.TryValidateIdentifier(targetSchema, "targetSchema", schemaResult: null, testResult: result, allowEmpty: false) == false)
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
            // Surface the ambiguity so Studio can flag the duplicate before the user saves the CDC task.
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
        if (CdcSinkRequestValidation.TryValidateIdentifier(targetTable.SourceTableSchema, $"{nameof(CdcSinkTableConfig)}.{nameof(CdcSinkTableConfig.SourceTableSchema)}", schemaResult: null, testResult: result) == false ||
            CdcSinkRequestValidation.TryValidateIdentifier(targetTable.SourceTableName, $"{nameof(CdcSinkTableConfig)}.{nameof(CdcSinkTableConfig.SourceTableName)}", schemaResult: null, testResult: result, allowEmpty: false) == false)
            return result;
        if (targetTable.PrimaryKeyColumns != null)
        {
            foreach (var pkColumn in targetTable.PrimaryKeyColumns)
            {
                if (CdcSinkRequestValidation.TryValidateIdentifier(pkColumn, $"{nameof(CdcSinkTableConfig)}.{nameof(CdcSinkTableConfig.PrimaryKeyColumns)}", schemaResult: null, testResult: result, allowEmpty: false) == false)
                    return result;
            }
        }
        if (targetTable.Columns != null)
        {
            foreach (var column in targetTable.Columns)
            {
                if (CdcSinkRequestValidation.TryValidateIdentifier(column.Column, $"{nameof(CdcColumnMapping)}.{nameof(CdcColumnMapping.Column)}", schemaResult: null, testResult: result, allowEmpty: false) == false)
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
            // The admin caller is interested in the full driver message - host, port, internal
            // error code - so they can diagnose the source-side problem directly. Log a warning
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
            RequestHandler.Database, context, request.Configuration, targetTable,
            fetched.ColumnNames, fetched.Rows, request.Operation, defaultSchema);
    }

    private SqlConnectionString ResolveTestConnection(TestCdcSinkMappingRequest request)
        => CdcSinkRequestValidation.ResolveSqlConnection(
            RequestHandler.Database,
            request.Connection,
            request.Configuration?.ConnectionStringName,
            inlineFieldName: nameof(TestCdcSinkMappingRequest.Connection),
            namedFieldName: nameof(CdcSinkConfiguration.ConnectionStringName));
}
