using System;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ShardedHandlers.Processors;
internal class ShardedConfigurationHandlerProcessorForGetStudioConfiguration : AbstractStudioConfigurationHandlerProcessor<ShardedRequestHandler, TransactionOperationContext>
{
    private readonly ShardedContext _shardedContext;

    public ShardedConfigurationHandlerProcessorForGetStudioConfiguration(ShardedRequestHandler requestHandler, [NotNull] ShardedContext shardedContext)
        : base(requestHandler, requestHandler.ContextPool)
    {
        _shardedContext = shardedContext ?? throw new ArgumentNullException(nameof(shardedContext));
    }

    protected override StudioConfiguration GetStudioConfiguration()
    {
        return _shardedContext.DatabaseRecord.Studio;
    }
}
