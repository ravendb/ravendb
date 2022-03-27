using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Admin.Processors.Configuration;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Admin.Processors.Configuration;

internal class ShardedAdminConfigurationHandlerProcessorForGetDatabaseRecord : AbstractHandlerProcessorForGetDatabaseRecord<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedAdminConfigurationHandlerProcessorForGetDatabaseRecord([NotNull] ShardedDatabaseRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override string GetDatabaseName() => RequestHandler.DatabaseContext.DatabaseName;
}
