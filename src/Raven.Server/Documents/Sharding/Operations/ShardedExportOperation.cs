using System;
using System.IO;
using System.IO.Compression;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors.Smuggler;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Handlers.Processors.Smuggler;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Operations
{
    public readonly struct ShardedExportOperation : IShardedOperation<BlittableJsonReaderObject>
    {
        private readonly ShardedDatabaseRequestHandler _handler;
        private readonly DatabaseSmugglerOptionsServerSide _options;
        private readonly AsyncBlittableJsonTextWriter _writer;
        private readonly long _operationId;

        public ShardedExportOperation(ShardedDatabaseRequestHandler handler, long operationId, DatabaseSmugglerOptionsServerSide options, AsyncBlittableJsonTextWriter writer)
        {
            _handler = handler;
            _options = options;
            _writer = writer;
            _operationId = operationId;
        }
        public RavenCommand<BlittableJsonReaderObject> CreateCommandForShard(int shardNumber) => new ShardedExportCommand(_handler, _operationId, _options, _writer);

        public HttpRequest HttpRequest => _handler.HttpContext.Request;
        public BlittableJsonReaderObject Combine(Memory<BlittableJsonReaderObject> results) => null;

        private class ShardedExportCommand : ShardedStreamCommand
        {
            private readonly long _operationId;
            private readonly DatabaseSmugglerOptionsServerSide _options;
            private readonly AsyncBlittableJsonTextWriter _writer;

            public ShardedExportCommand(ShardedDatabaseRequestHandler handler, long operationId, DatabaseSmugglerOptionsServerSide options, AsyncBlittableJsonTextWriter writer) : 
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
                await using (var gzipStream = new GZipStream(ShardedSmugglerHandlerProcessorForExport.GetInputStream(stream, _options), CompressionMode.Decompress))
                {
                    await _writer.WriteStreamAsync(gzipStream);
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
