using JetBrains.Annotations;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Configuration;

internal class ConfigurationHandlerProcessorForGetTimeSeriesConfiguration : AbstractConfigurationHandlerProcessorForGetTimeSeriesConfiguration<DatabaseRequestHandler, DocumentsOperationContext>
{
    public ConfigurationHandlerProcessorForGetTimeSeriesConfiguration([NotNull] DatabaseRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override TimeSeriesConfiguration GetTimeSeriesConfiguration()
    {
        using (RequestHandler.Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            TimeSeriesConfiguration timeSeriesConfig;
            using (var rawRecord = RequestHandler.Server.ServerStore.Cluster.ReadRawDatabaseRecord(context, RequestHandler.Database.Name))
            {
                timeSeriesConfig = rawRecord?.TimeSeriesConfiguration;
            }
            return timeSeriesConfig;
        }
    }
}
