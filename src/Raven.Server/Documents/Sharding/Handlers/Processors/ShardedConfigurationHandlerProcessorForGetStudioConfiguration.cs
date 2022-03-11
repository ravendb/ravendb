using Raven.Client.Documents.Operations.Configuration;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors;
internal class ShardedConfigurationHandlerProcessorForGetStudioConfiguration : AbstractConfigurationHandlerProcessorForGetStudioConfiguration<ShardedRequestHandler, TransactionOperationContext>
{
    private readonly ShardedContext _shardedContext;

    public ShardedConfigurationHandlerProcessorForGetStudioConfiguration(ShardedRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
        _shardedContext = requestHandler.ShardedContext;
    }

    protected override StudioConfiguration GetStudioConfiguration()
    {
        return _shardedContext.DatabaseRecord.Studio;
    }
}
