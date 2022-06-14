using System;
using System.IO.Compression;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents.Sharding.Handlers.Processors.Smuggler;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Operations
{
    public readonly struct ShardedExportOperation : IShardedOperation<BlittableJsonReaderObject>
    {
        private readonly DatabaseSmugglerOptionsServerSide _options;
        private readonly AsyncBlittableJsonTextWriter _writer;
        private readonly long _operationId;

        public ShardedExportOperation(HttpRequest httpRequest, DatabaseSmugglerOptionsServerSide options, AsyncBlittableJsonTextWriter writer, long operationId)
        {
            HttpRequest = httpRequest;
            _options = options;
            _writer = writer;
            _operationId = operationId;
        }

        public RavenCommand<BlittableJsonReaderObject> CreateCommandForShard(int shardNumber) => new ShardedExportCommand(_options, _writer, _operationId);

        public HttpRequest HttpRequest { get; }

        public BlittableJsonReaderObject Combine(Memory<BlittableJsonReaderObject> results) => null;

        private class ShardedExportCommand : RavenCommand<BlittableJsonReaderObject>
        {
            private readonly long _operationId;
            private readonly DatabaseSmugglerOptionsServerSide _options;
            private readonly AsyncBlittableJsonTextWriter _writer;

            public ShardedExportCommand(DatabaseSmugglerOptionsServerSide options, AsyncBlittableJsonTextWriter writer, long operationId)
            {
                _operationId = operationId;
                _options = options;
                _writer = writer;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/smuggler/export?operationId={_operationId}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_options, ctx)).ConfigureAwait(false);
                    })
                };
            }

            public override async Task<ResponseDisposeHandling> ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
            {
                await using (var stream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false))
                await using (var gzipStream = new GZipStream(ShardedSmugglerHandlerProcessorForExport.GetInputStream(stream, _options), CompressionMode.Decompress))
                {
                    await _writer.WriteStreamAsync(gzipStream).ConfigureAwait(false);
                }

                return ResponseDisposeHandling.Automatic;
            }
        }
    }
}
