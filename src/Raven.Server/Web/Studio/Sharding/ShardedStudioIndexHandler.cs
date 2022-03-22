using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Studio.Processors;
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

    [RavenAction("/databases/*/studio/index-type", "POST", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task PostIndexType()
    {
        using (var processor = new StudioIndexHandlerForPostIndexType<TransactionOperationContext>(this, ContextPool))
            await processor.ExecuteAsync();
    }

    [RavenAction("/databases/*/studio/index-fields", "POST", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task PostIndexFields()
    {
        using (var processor = new ShardedStudioIndexHandlerForPostIndexFields(this))
            await processor.ExecuteAsync();
    }
}
