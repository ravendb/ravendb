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
/// Reads the CDC sink performance stats RavenDB exposes at
/// <c>GET /databases/{db}/cdc-sink/performance</c> (the same feed Studio's CDC stats view
/// uses) and parses it into <see cref="CdcSinkPerformanceRaw"/>. The stats are a rolling
/// window of the last ~25 batches per sink process and stay empty until the server collects
/// them (RavenDB-26780 / ravendb#23046) and a batch has run. Read raw + System.Text.Json,
/// mirroring <see cref="Agents.RunDraftAgentTestOperation"/>, since RavenDB's blittable sync
/// reader is internal to the client assembly.
/// </summary>
internal sealed class GetCdcSinkPerformanceStatisticsOperation : IMaintenanceOperation<CdcSinkPerformanceRaw>
{
    public RavenCommand<CdcSinkPerformanceRaw> GetCommand(DocumentConventions conventions, JsonOperationContext ctx) =>
        new Command();

    private sealed class Command : RavenCommand<CdcSinkPerformanceRaw>
    {
        private static readonly JsonSerializerOptions SerializerOptions = new() { PropertyNameCaseInsensitive = true };

        public Command()
        {
            ResponseType = RavenCommandResponseType.Raw;
        }

        public override bool IsReadRequest => true;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            // The CDC sink's rolling perf window lives on whichever node runs the sink; this hits
            // the request-executor-selected node, so on a multi-node cluster the snapshot can be
            // empty/partial. Fine for the single-node appliance and mirrors Studio (review B5).
            url = $"{node.Url}/databases/{node.Database}/cdc-sink/performance";
            return new HttpRequestMessage { Method = HttpMethod.Get };
        }

        public override async Task SetResponseRawAsync(HttpResponseMessage response, Stream stream, JsonOperationContext context)
        {
            Result = await JsonSerializer.DeserializeAsync<CdcSinkPerformanceRaw>(stream, SerializerOptions)
                .ConfigureAwait(false) ?? new CdcSinkPerformanceRaw();
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            // Not used on the Raw path, but RavenCommand requires it.
            Result ??= new CdcSinkPerformanceRaw();
        }
    }
}
