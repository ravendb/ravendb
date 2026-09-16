using System.IO;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Quill.Cdc;

/// <summary>
/// Reads the CDC sink error details RavenDB exposes at
/// <c>GET /databases/{db}/cdc-sink/errors</c> (the same feed Studio's task-errors view uses)
/// and parses it into <see cref="CdcSinkErrorsRaw"/>. Read raw + System.Text.Json, mirroring
/// <see cref="GetCdcSinkPerformanceStatisticsOperation"/>, since RavenDB's blittable sync
/// reader is internal to the client assembly.
/// </summary>
internal sealed class GetCdcSinkErrorsOperation : IMaintenanceOperation<CdcSinkErrorsRaw>
{
    public RavenCommand<CdcSinkErrorsRaw> GetCommand(DocumentConventions conventions, JsonOperationContext ctx) =>
        new Command();

    private sealed class Command : RavenCommand<CdcSinkErrorsRaw>
    {
        private static readonly JsonSerializerOptions SerializerOptions = new() { PropertyNameCaseInsensitive = true };

        public Command()
        {
            ResponseType = RavenCommandResponseType.Raw;
        }

        public override bool IsReadRequest => true;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            // Like the perf feed, the error store lives on whichever node runs the sink; this hits
            // the request-executor-selected node, so on a multi-node cluster the result can be
            // empty/partial. Fine for the single-node appliance and mirrors Studio.
            url = $"{node.Url}/databases/{node.Database}/cdc-sink/errors";
            return new HttpRequestMessage { Method = HttpMethod.Get };
        }

        public override async Task SetResponseRawAsync(HttpResponseMessage response, Stream stream, JsonOperationContext context)
        {
            Result = await JsonSerializer.DeserializeAsync<CdcSinkErrorsRaw>(stream, SerializerOptions)
                .ConfigureAwait(false) ?? new CdcSinkErrorsRaw();
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            // Not used on the Raw path, but RavenCommand requires it.
            Result ??= new CdcSinkErrorsRaw();
        }
    }
}
