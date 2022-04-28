using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Admin.Processors.TimeSeries;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Admin.Processors.TimeSeries
{
    internal class ShardedAdminTimeSeriesHandlerProcessorForPutTimeSeriesPolicy : AbstractAdminTimeSeriesHandlerProcessorForPutTimeSeriesPolicy<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedAdminTimeSeriesHandlerProcessorForPutTimeSeriesPolicy([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
    }
}
