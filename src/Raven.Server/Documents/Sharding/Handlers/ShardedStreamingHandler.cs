using System.Net.Http;
using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Streaming;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    internal class ShardedStreamingHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/streams/docs", "GET")]
        public async Task StreamDocsGet()
        {
            using (var processor = new ShardedStreamingHandlerProcessorForGetDocs(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/streams/queries", "GET")]
        public async Task StreamQueryGet()
        {
            using (var processor = new ShardedStreamingHandlerProcessorForGetStreamQuery(this, HttpMethod.Get))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/streams/queries", "POST")]
        public async Task StreamQueryPost()
        {
            using (var processor = new ShardedStreamingHandlerProcessorForGetStreamQuery(this, HttpMethod.Post))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/streams/queries", "HEAD")]
        public Task SteamQueryHead()
        {
            return Task.CompletedTask;
        }
    }
}
