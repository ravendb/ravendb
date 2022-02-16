using System;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding;
using Sparrow.Json;

namespace Raven.Server.Documents.ShardedHandlers.ShardedCommands
{
    public abstract class ShardedStreamCommand : ShardedCommand
    {
        protected ShardedStreamCommand(ShardedRequestHandler handler, BlittableJsonReaderObject content) : base(handler, ShardedCommands.Headers.IfMatch, content)
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
