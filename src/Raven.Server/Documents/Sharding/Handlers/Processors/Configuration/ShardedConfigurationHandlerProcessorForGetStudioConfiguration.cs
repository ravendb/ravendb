using Raven.Client.Documents.Operations.Configuration;
using Raven.Server.Documents.Handlers.Processors.Configuration;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Configuration;
internal class ShardedConfigurationHandlerProcessorForGetStudioConfiguration : AbstractConfigurationHandlerProcessorForGetStudioConfiguration<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedConfigurationHandlerProcessorForGetStudioConfiguration(ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override StudioConfiguration GetStudioConfiguration() => RequestHandler.DatabaseContext.DatabaseRecord.Studio;
}
