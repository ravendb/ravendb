using System;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.CdcSink.Schema;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.CdcSink.Handlers.Processors;

internal sealed class CdcSinkHandlerProcessorForSchema : AbstractCdcSinkHandlerProcessorForSchema<DatabaseRequestHandler, DocumentsOperationContext>
{
    public CdcSinkHandlerProcessorForSchema([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        using (var cts = CancellationTokenSource.CreateLinkedTokenSource(RequestHandler.Database.DatabaseShutdown, HttpContext.RequestAborted))
        using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var bodyJson = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "CdcSinkSchemaRequest");
            var request = JsonDeserializationClient.CdcSinkSchemaRequest(bodyJson);

            var result = await ExecuteSchemaDiscoveryAsync(request, cts.Token);

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
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
                if (CdcSinkRequestValidation.TryValidateIdentifier(schemaName, $"{nameof(CdcSinkSchemaRequest.Schemas)}[]", schemaResult: result, testResult: null) == false)
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

        CdcSinkSourceSchema schema;
        try
        {
            schema = await discovery.DiscoverAsync(connection.ConnectionString, request.Schemas, ct);
        }
        catch (Exception e)
        {
            result.Errors.Add("Schema discovery against the source database failed: " + e);
            if (Logger.IsWarnEnabled)
                Logger.Warn("CDC schema discovery failed", e);
            return result;
        }

        // Discovery succeeded - fold connection-level + per-table verification into the same
        // response so callers don't need a second round-trip. Verification runs only after a
        // successful discovery; on discovery failure we already returned above with a single
        // structured error.
        try
        {
            await CdcSinkSourceVerifier.AnnotateAsync(connection, schema, ct);
        }
        catch (Exception e)
        {
            schema.Errors.Add("Source verification failed: " + e);
            if (Logger.IsWarnEnabled)
                Logger.Warn("CDC source verification failed", e);
        }

        return schema;
    }

    /// <summary>
    /// Inline <see cref="CdcSinkSchemaRequest.Connection"/> takes precedence - Studio's Task
    /// Creation view sends raw credentials because the connection-string named record may
    /// not exist in <c>databaseRecord.SqlConnectionStrings</c> yet. Falls back to the named
    /// lookup for post-save callers.
    /// </summary>
    private SqlConnectionString ResolveConnection(CdcSinkSchemaRequest request)
        => CdcSinkRequestValidation.ResolveSqlConnection(
            RequestHandler.Database,
            request.Connection,
            request.ConnectionStringName,
            inlineFieldName: nameof(CdcSinkSchemaRequest.Connection),
            namedFieldName: nameof(CdcSinkSchemaRequest.ConnectionStringName));
}
