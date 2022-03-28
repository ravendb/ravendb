using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.Documents.Handlers.Processors.Configuration;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Configuration;
internal class ShardedConfigurationHandlerProcessorForGetTimeSeriesConfiguration : AbstractConfigurationHandlerProcessorForGetTimeSeriesConfiguration<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedConfigurationHandlerProcessorForGetTimeSeriesConfiguration(ShardedDatabaseRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override TimeSeriesConfiguration GetTimeSeriesConfiguration()
    {
        return RequestHandler.DatabaseContext.DatabaseRecord.TimeSeries;
    }
}
