using System;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding.Handlers;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Commands
{
    internal class GetRawRevisionsCommand : ShardedStreamCommand
    {
        private readonly ShardedDatabaseRequestHandler _handler;
        public override bool IsReadRequest => true;

        private readonly GetRevisionsCommand _cmd;

        public GetRawRevisionsCommand(ShardedDatabaseRequestHandler handler, string id, int? start, int? pageSize, bool metadataOnly = false) : base(handler, null)
        {
            _handler = handler;
            _cmd = new GetRevisionsCommand(id, start, pageSize, metadataOnly);
        }

        public GetRawRevisionsCommand(ShardedDatabaseRequestHandler handler, string id, DateTime before) : base(handler, null)
        {
            _handler = handler;
            _cmd = new GetRevisionsCommand(id, before);
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            return _cmd.CreateRequest(ctx, node, out url);
        }
        
        public override async Task HandleStreamResponse(Stream responseStream)
        {
            await responseStream.CopyToAsync(_handler.ResponseBodyStream());
        }
    }
}
