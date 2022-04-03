using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding.Handlers;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Commands
{
    internal class GetDocumentConflictsCommand : ShardedStreamCommand
    {
        private readonly ShardedDatabaseRequestHandler _handler;
        public override bool IsReadRequest => true;

        private readonly GetConflictsCommand _cmd;

        public GetDocumentConflictsCommand(ShardedDatabaseRequestHandler handler, string id) : base(handler, null)
        {
            _handler = handler;
            _cmd = new GetConflictsCommand(id);
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
