using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Server.Documents.Handlers.Processors.Configuration;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Configuration;

internal class ShardedConfigurationHandlerProcessorForGetClientConfiguration : AbstractConfigurationHandlerProcessorForGetClientConfiguration<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedConfigurationHandlerProcessorForGetClientConfiguration([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override ClientConfiguration GetDatabaseClientConfiguration() => RequestHandler.DatabaseContext.DatabaseRecord.Client;
}
