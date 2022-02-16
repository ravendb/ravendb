using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.ShardedHandlers.ShardedCommands
{
    public readonly struct ShardedExportOperation : IShardedOperation<BlittableJsonReaderObject>
    {
        private readonly ShardedRequestHandler _handler;
        private readonly DatabaseSmugglerOptionsServerSide _options;
        private readonly AsyncBlittableJsonTextWriter _writer;
        private readonly long _operationId;

        public ShardedExportOperation(ShardedRequestHandler handler, long operationId, DatabaseSmugglerOptionsServerSide options, AsyncBlittableJsonTextWriter writer)
        {
            _handler = handler;
            _options = options;
            _writer = writer;
            _operationId = operationId;
        }
        public RavenCommand<BlittableJsonReaderObject> CreateCommandForShard(int shard) => new ShardedExportCommand(_handler, _operationId, _options, _writer);

        public BlittableJsonReaderObject Combine(Memory<BlittableJsonReaderObject> results) => null;

        private class ShardedExportCommand : ShardedStreamCommand
        {
            private readonly long _operationId;
            private readonly DatabaseSmugglerOptionsServerSide _options;
            private readonly AsyncBlittableJsonTextWriter _writer;

            public ShardedExportCommand(ShardedRequestHandler handler, long operationId, DatabaseSmugglerOptionsServerSide options, AsyncBlittableJsonTextWriter writer) : 
                base(handler, content: null)
            {
                _operationId = operationId;
                _options = options;
                _writer = writer;

                var queryString = HttpUtility.ParseQueryString(handler.HttpContext.Request.QueryString.Value);
                queryString["operationId"] = _operationId.ToString();
                Url = handler.BaseShardUrl + "?" + queryString;
            }

            public override async Task HandleStreamResponse(Stream stream)
            {
                await using (var gzipStream = new GZipStream(RequestHandler.GetInputStream(stream, _options), CompressionMode.Decompress))
                {
                    _writer.WriteStream(gzipStream);
                }
            } 

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                Content = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_options, ctx);
                return base.CreateRequest(ctx, node, out url);
            }
        }
    }
}
