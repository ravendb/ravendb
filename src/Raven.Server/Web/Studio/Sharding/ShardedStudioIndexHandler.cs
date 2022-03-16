using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Routing;
using Raven.Server.Web.Studio.Sharding.Processors;

namespace Raven.Server.Web.Studio.Sharding;

public class ShardedStudioIndexHandler : ShardedRequestHandler
{
    [RavenShardedAction("/databases/*/studio/indexes/errors-count", "GET")]
    public async Task PreviewCollection()
    {
        using (var processor = new ShardedStudioIndexHandlerProcessorForGetIndexErrorsCount(this))
            await processor.ExecuteAsync();
    }
}
