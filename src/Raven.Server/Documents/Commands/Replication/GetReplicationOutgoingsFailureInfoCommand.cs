using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors.Replication;
using Raven.Server.Json;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Commands.Replication
{
    internal sealed class GetReplicationOutgoingsFailureInfoCommand : RavenCommand<ReplicationOutgoingsFailurePreview>
    {
        public GetReplicationOutgoingsFailureInfoCommand()
        {
        }

        public GetReplicationOutgoingsFailureInfoCommand(string nodeTag)
        {
            SelectedNodeTag = nodeTag;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/replication/debug/outgoing-failures";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };

            return request;
        }

        public override async Task<ResponseDisposeHandling> ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
        {
            await using var stream = await response.Content.ReadAsStreamWithZstdSupportAsync().ConfigureAwait(false);
            using var memoryStream = RecyclableMemoryStreamFactory.GetRecyclableStream();
            await stream.CopyToAsync(memoryStream);

            memoryStream.Position = 0;
            var state = new JsonParserState();

            using (var parser = new UnmanagedJsonParser(context, state, "outgoing-failures/response"))
            using (context.GetMemoryBuffer(out var buffer))
            using (var peepingTomStream = new PeepingTomStream(memoryStream, context))
            {
                var builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, "read/metadata", parser, state);

                // read metadata
                await UnmanagedJsonParserHelper.ReadAsync(peepingTomStream, parser, state, buffer);
                await UnmanagedJsonParserHelper.ReadPropertyAsync(builder, peepingTomStream, parser, buffer);

                // read Stats
                builder.Renew("read/stats", BlittableJsonDocumentBuilder.UsageMode.None);
                await UnmanagedJsonParserHelper.ReadPropertyAsync(builder, peepingTomStream, parser, buffer, CancellationToken.None);

                var res = builder.CreateReader();

                SetResponse(context, res, false);
            }

            return ResponseDisposeHandling.Automatic;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = JsonDeserializationServer.ReplicationOutgoingsFailurePreview(response);
        }

        public override bool IsReadRequest => true;
    }
}
