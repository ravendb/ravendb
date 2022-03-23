using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding.Handlers;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Commands
{
    public abstract class ShardedStreamCommand : ShardedCommand
    {
        protected ShardedStreamCommand(ShardedDatabaseRequestHandler handler, BlittableJsonReaderObject content) : base(handler, Commands.Headers.IfMatch, content)
        {
        }

        public abstract Task HandleStreamResponse(Stream responseStream);

        public override async Task<ResponseDisposeHandling> ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
        {
            using (var stream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false))
            {
                await HandleStreamResponse(stream).ConfigureAwait(false);
            }

            return ResponseDisposeHandling.Automatic;
        }
    }
}
