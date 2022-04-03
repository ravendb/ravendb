using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Web.Operations;
using Sparrow.Json;

namespace Raven.Server.Web.Studio.Sharding.Commands
{
    internal class GetConflictResolutionCommand : ShardedStreamCommand
    {
        private readonly ShardedDatabaseRequestHandler _handler;
        public override bool IsReadRequest => true;

        private readonly GetSuggestConflictResolutionOperation.GetSuggestConflictResolutionCommand _cmd;

        public GetConflictResolutionCommand(ShardedDatabaseRequestHandler handler, string id) : base(handler, null)
        {
            _handler = handler;
            _cmd = new GetSuggestConflictResolutionOperation.GetSuggestConflictResolutionCommand(id);
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
