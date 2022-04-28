using Raven.Server.Documents.Handlers.Admin.Processors.Configuration;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Admin.Processors.Configuration
{
    internal class ShardedAdminConfigurationHandlerProcessorForPutStudioConfiguration : AbstractAdminConfigurationHandlerProcessorForPutStudioConfiguration<TransactionOperationContext>
    {
        public ShardedAdminConfigurationHandlerProcessorForPutStudioConfiguration(ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
    }
}
