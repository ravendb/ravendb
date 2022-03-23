using System;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Commands
{
    internal struct ShardedImportOperation : IShardedOperation<BlittableJsonReaderObject>
    {
        private readonly ShardedDatabaseRequestHandler _handler;
        private readonly MultiShardedDestination.StreamDestinationHolder[] _holders;
        private readonly DatabaseSmugglerOptionsServerSide _options;
        public readonly Task<Stream>[] ExposedStreamTasks;

        public ShardedImportOperation(ShardedDatabaseRequestHandler handler, MultiShardedDestination.StreamDestinationHolder[] holders, DatabaseSmugglerOptionsServerSide options)
        {
            _handler = handler;
            _holders = holders;
            _options = options;
            ExposedStreamTasks = new Task<Stream>[_holders.Length];

            for (int i = 0; i < _holders.Length; i++)
            {
                var stream = new StreamExposerContent();
                _holders[i].OutStream = stream;
                ExposedStreamTasks[i] = stream.OutputStream;
            }
        }

        public BlittableJsonReaderObject Combine(Memory<BlittableJsonReaderObject> results) => null;

        public RavenCommand<BlittableJsonReaderObject> CreateCommandForShard(int shard) => new ShardedImportCommand(_handler, _holders[shard].OutStream, _options);
    }

    internal class ShardedImportCommand : ShardedCommand
    {
        private readonly StreamExposerContent _stream;
        private readonly DatabaseSmugglerOptionsServerSide _options;

        public ShardedImportCommand(ShardedDatabaseRequestHandler handler, StreamExposerContent stream, DatabaseSmugglerOptionsServerSide options) : base(handler, Commands.Headers.None)
        {
            _stream = stream;
            _options = options;

            var queryString = HttpUtility.ParseQueryString(handler.HttpContext.Request.QueryString.Value);
            Url = "/smuggler/import?" + queryString;
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
                    _stream, "file", "name"
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
