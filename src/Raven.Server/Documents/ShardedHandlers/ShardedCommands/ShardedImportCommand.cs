using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents.Sharding;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;

namespace Raven.Server.Documents.ShardedHandlers.ShardedCommands
{
    public struct ShardedImportOperation : IShardedOperation<BlittableJsonReaderObject>
    {
        private readonly ShardedRequestHandler _handler;
        private readonly List<Stream> _streams;
        private readonly DatabaseSmugglerOptionsServerSide _options;

        public ShardedImportOperation(ShardedRequestHandler handler, List<Stream> streams, DatabaseSmugglerOptionsServerSide options)
        {
            _handler = handler;
            _streams = streams;
            _options = options;
        }

        public BlittableJsonReaderObject Combine(Memory<BlittableJsonReaderObject> results) => null;

        public RavenCommand<BlittableJsonReaderObject> CreateCommandForShard(int shard) => new ShardedImportCommand(_handler, _streams[shard], _options);
    }

    public class ShardedImportCommand : ShardedCommand
    {
        private readonly Stream _stream;
        private readonly DatabaseSmugglerOptionsServerSide _options;

        public ShardedImportCommand(ShardedRequestHandler handler, Stream stream, DatabaseSmugglerOptionsServerSide options) : base(handler, ShardedCommands.Headers.None)
        {
            _stream = stream;
            _options = options;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var multi = new MultipartFormDataContent
            {
                {
                    new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_options, ctx))),
                    Constants.Smuggler.ImportOptions
                },
                {
                    new StreamContent(_stream), "file", "name"
                }
            };

            url = $"{node.Url}/databases/{node.Database}{Url}";
            var message = new HttpRequestMessage
            {
                Headers =
                {
                    TransferEncodingChunked = true
                },
                Method = Method,
                Content = multi,
            };
            return message;
        }
    }
}
