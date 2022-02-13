using System;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding;
using Sparrow.Json;

namespace Raven.Server.Documents.ShardedHandlers.ShardedCommands
{
    public class ShardedStreamCommand : ShardedCommand
    {
        private readonly Func<Stream, Task> _handleStreamResponse;

        public ShardedStreamCommand(ShardedRequestHandler handler, Func<Stream, Task> handleStreamResponse, BlittableJsonReaderObject content) : base(handler, ShardedCommands.Headers.IfMatch, content)
        {
            _handleStreamResponse = handleStreamResponse;
        }

        public override async Task<ResponseDisposeHandling> ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
        {
            using (var stream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false))
            {
                await _handleStreamResponse(stream).ConfigureAwait(false);
            }

            return ResponseDisposeHandling.Automatic;
        }
    }
}
