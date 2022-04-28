using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Admin.Processors.Configuration;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Admin.Processors.Configuration;

internal class ShardedAdminConfigurationHandlerProcessorForPutClientConfiguration : AbstractAdminConfigurationHandlerProcessorForPutClientConfiguration<TransactionOperationContext>
{
    public ShardedAdminConfigurationHandlerProcessorForPutClientConfiguration(ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }
}
