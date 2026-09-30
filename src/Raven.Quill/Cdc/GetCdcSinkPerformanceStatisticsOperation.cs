using System.IO;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Quill.Cdc;

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

        // single-node snapshot; on a cluster this node may be partial (B5)
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
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
            Result ??= new CdcSinkPerformanceRaw();
        }
    }
}
