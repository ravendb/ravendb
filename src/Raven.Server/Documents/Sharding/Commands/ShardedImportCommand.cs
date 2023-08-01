using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Commands
{
    internal readonly struct ShardedImportOperation : IShardedOperation<BlittableJsonReaderObject>
    {
        private readonly Dictionary<int, MultiShardedDestination.StreamDestinationHolder> _holders;
        private readonly long _operationId;
        private readonly DatabaseSmugglerOptionsServerSide _options;
        public readonly Dictionary<int, Task<Stream>> ExposedStreamTasks;

        public ShardedImportOperation(HttpRequest httpRequest, DatabaseSmugglerOptionsServerSide options, Dictionary<int, MultiShardedDestination.StreamDestinationHolder> holders, long operationId)
        {
            HttpRequest = httpRequest;
            _holders = holders;
            _operationId = operationId;
            _options = options;
            ExposedStreamTasks = new Dictionary<int, Task<Stream>>(_holders.Count);

            foreach (var shardNumber in holders.Keys)
            {
                var stream = new StreamExposerContent();
                _holders[shardNumber].OutStream = stream;
                ExposedStreamTasks[shardNumber] = stream.OutputStream;
            }
        }

        public HttpRequest HttpRequest { get; }

        public BlittableJsonReaderObject Combine(Dictionary<int, ShardExecutionResult<BlittableJsonReaderObject>> results) => null;

        public RavenCommand<BlittableJsonReaderObject> CreateCommandForShard(int shardNumber) => new ShardedImportCommand(_options, _holders[shardNumber].OutStream, _operationId);

        private sealed class ShardedImportCommand : RavenCommand<BlittableJsonReaderObject>
        {
            private readonly StreamExposerContent _stream;
            private readonly long _operationId;
            private readonly DatabaseSmugglerOptionsServerSide _options;

            public ShardedImportCommand(DatabaseSmugglerOptionsServerSide options, StreamExposerContent stream, long operationId)
            {
                _stream = stream;
                _operationId = operationId;
                _options = options;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/smuggler/import?operationId={_operationId}";

                return new HttpRequestMessage
                {
                    Headers =
                    {
                        TransferEncodingChunked = true
                    },
                    Method = HttpMethod.Post,
                    Content = new MultipartFormDataContent
                    {
                        {
                            new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_options, ctx)), DocumentConventions.DefaultForServer),
                            Constants.Smuggler.ImportOptions
                        },
                        {
                            _stream, "file", "name"
                        }
                    }
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = response;
            }
        }
    }
}
