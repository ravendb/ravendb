using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.CdcSink.Schema;

/// <summary>
/// Browses the source database's tables, columns, PKs, and FKs annotated with CDC-specific
/// hints (suggested column type, capturability, table CDC enrollment) so callers can render
/// the CDC mapping UI without a manual schema dump. Calls <c>POST /admin/cdc-sink/schema</c>.
/// Requires <c>DatabaseAdmin</c>.
/// </summary>
internal class GetCdcSinkSchemaOperation : IMaintenanceOperation<CdcSinkSourceSchema>
{
    private readonly CdcSinkSchemaRequest _request;

    /// <summary>
    /// Inline-credentials flavour. Use this from a Task Creation view where the connection
    /// hasn't been saved to <c>databaseRecord.SqlConnectionStrings</c> yet.
    /// </summary>
    public GetCdcSinkSchemaOperation(SqlConnectionString connection, string[] schemas = null)
        : this(new CdcSinkSchemaRequest { Connection = connection ?? throw new ArgumentNullException(nameof(connection)), Schemas = schemas })
    {
    }

    /// <summary>
    /// Named-lookup flavour. Use this from post-save callers — the server resolves
    /// <paramref name="connectionStringName"/> against the database record.
    /// </summary>
    public GetCdcSinkSchemaOperation(string connectionStringName, string[] schemas = null)
        : this(new CdcSinkSchemaRequest { ConnectionStringName = connectionStringName ?? throw new ArgumentNullException(nameof(connectionStringName)), Schemas = schemas })
    {
    }

    public GetCdcSinkSchemaOperation(CdcSinkSchemaRequest request)
    {
        _request = request ?? throw new ArgumentNullException(nameof(request));
    }

    public RavenCommand<CdcSinkSourceSchema> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
    {
        return new GetCdcSinkSchemaCommand(conventions, _request);
    }

    private sealed class GetCdcSinkSchemaCommand : RavenCommand<CdcSinkSourceSchema>
    {
        private readonly CdcSinkSchemaRequest _request;
        private readonly DocumentConventions _conventions;

        public GetCdcSinkSchemaCommand(DocumentConventions conventions, CdcSinkSchemaRequest request)
        {
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            _request = request ?? throw new ArgumentNullException(nameof(request));
        }

        // POST verb but no server-side state change — allow FastestNode failover.
        public override bool IsReadRequest => true;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/admin/cdc-sink/schema";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(
                    async stream => await ctx.WriteAsync(stream, ctx.ReadObject(_request.ToJson(), "CdcSinkSchemaRequest")).ConfigureAwait(false),
                    _conventions),
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = JsonDeserializationClient.CdcSinkSourceSchema(response);
        }
    }
}
