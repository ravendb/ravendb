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
    }
}
