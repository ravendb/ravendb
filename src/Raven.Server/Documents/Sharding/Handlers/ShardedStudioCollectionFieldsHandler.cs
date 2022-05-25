using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Studio;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedStudioCollectionFieldsHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/studio/collections/fields", "GET")]
        public async Task GetCollectionFields()
        {
            using (var processor = new ShardedStudioCollectionFieldsHandlerProcessorForGetCollectionFields(this))
                await processor.ExecuteAsync();
        }
    }
}
