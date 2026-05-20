using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.AiAppliance.Wizard;

/// <summary>
/// Calls <c>POST /admin/cdc-sink/verify</c>. Mirrors the schema of the
/// corresponding server-side endpoint (which has no client-side operation today
/// — <c>VerifyCdcSinkOperation</c> is the suggested follow-up on the RavenDB
/// server, after which this class is deleted).
/// </summary>
internal sealed class CdcSinkVerifyOperation : IMaintenanceOperation<ConnectResult>
{
    private readonly string _connectionStringName;
    private readonly List<string>? _tableNames;

    public CdcSinkVerifyOperation(string connectionStringName, List<string>? tableNames = null)
    {
        _connectionStringName = connectionStringName ?? throw new ArgumentNullException(nameof(connectionStringName));
        _tableNames = tableNames;
    }

    public RavenCommand<ConnectResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx) =>
        new VerifyCommand(conventions, _connectionStringName, _tableNames);

    private sealed class VerifyCommand(DocumentConventions conventions, string connectionStringName, List<string>? tableNames)
        : RavenCommand<ConnectResult>
    {
        // POST with no server-side state change.
        public override bool IsReadRequest => true;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/admin/cdc-sink/verify";

            var body = new DynamicJsonValue
            {
                ["ConnectionStringName"] = connectionStringName,
                ["TableNames"] = tableNames == null ? null : new DynamicJsonArray(tableNames),
            };

            return new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(
                    async stream => await ctx.WriteAsync(stream, ctx.ReadObject(body, "CdcSinkVerifyRequest")).ConfigureAwait(false),
                    conventions),
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
            {
                ThrowInvalidResponse();
                return;
            }

            var result = new ConnectResult();

            if (response.TryGet("Success", out bool success))
                result.Success = success;

            if (response.TryGet("HasPermissionToSetup", out bool hasPermission))
                result.HasPermissionToSetup = hasPermission;

            if (response.TryGet("Errors", out BlittableJsonReaderArray? errors) && errors != null)
                foreach (var item in errors)
                    if (item != null)
                        result.Errors.Add(item.ToString()!);

            if (response.TryGet("Warnings", out BlittableJsonReaderArray? warnings) && warnings != null)
                foreach (var item in warnings)
                    if (item != null)
                        result.Warnings.Add(item.ToString()!);

            Result = result;
        }
    }
}
