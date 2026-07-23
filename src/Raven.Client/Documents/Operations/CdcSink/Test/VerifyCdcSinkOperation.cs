using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.CdcSink.Test;

/// <summary>
/// Runs the real CDC pull flow once against the configured source to verify CDC is set up correctly —
/// reading one row from each configured table and reporting which tables were reached, without persisting
/// anything (no task, documents, checkpoint or state) and undoing any CDC setup it provisions on the source.
/// Calls <c>POST /admin/cdc-sink/dry-run</c>. Requires <c>DatabaseAdmin</c>.
/// </summary>
internal class VerifyCdcSinkOperation : IMaintenanceOperation<CdcTestResult>
{
    private readonly CdcTestRequest _request;

    public VerifyCdcSinkOperation(CdcTestRequest request)
    {
        _request = request ?? throw new ArgumentNullException(nameof(request));
    }

    public RavenCommand<CdcTestResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
    {
        return new VerifyCdcSinkCommand(conventions, _request);
    }

    private sealed class VerifyCdcSinkCommand : RavenCommand<CdcTestResult>
    {
        private readonly CdcTestRequest _request;
        private readonly DocumentConventions _conventions;

        public VerifyCdcSinkCommand(DocumentConventions conventions, CdcTestRequest request)
        {
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            _request = request ?? throw new ArgumentNullException(nameof(request));
        }

        // The run provisions (and undoes) CDC on the source, so it is not a pure read - don't allow
        // FastestNode failover.
        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/admin/cdc-sink/dry-run";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(
                    async stream => await ctx.WriteAsync(stream, ctx.ReadObject(_request.ToJson(), "CdcTestRequest")).ConfigureAwait(false),
                    _conventions),
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = JsonDeserializationClient.CdcTestResult(response);
        }
    }
}
