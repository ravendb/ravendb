using JetBrains.Annotations;
using Raven.Server.Config;
using Raven.Server.Documents.Handlers.Admin.Processors.Configuration;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Admin.Processors.Configuration;

internal class ShardedAdminConfigurationHandlerProcessorForGetSettings : AbstractAdminConfigurationHandlerProcessorForGetSettings<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedAdminConfigurationHandlerProcessorForGetSettings([NotNull] ShardedDatabaseRequestHandler requestHandler) 
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override string GetDatabaseName() => RequestHandler.DatabaseContext.DatabaseName;

    protected override RavenConfiguration GetDatabaseConfiguration() => RequestHandler.DatabaseContext.Configuration;
}
