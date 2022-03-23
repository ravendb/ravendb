using Raven.Client.Documents.Operations.Configuration;
using Raven.Server.Documents.Handlers.Processors.Configuration;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Configuration;
internal class ShardedConfigurationHandlerProcessorForGetStudioConfiguration : AbstractConfigurationHandlerProcessorForGetStudioConfiguration<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    private readonly ShardedDatabaseContext _context;

    public ShardedConfigurationHandlerProcessorForGetStudioConfiguration(ShardedDatabaseRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
        _context = requestHandler.DatabaseContext;
    }

    protected override StudioConfiguration GetStudioConfiguration()
    {
        return _context.DatabaseRecord.Studio;
    }
}
