using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Admin.Processors.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Admin.Processors.TimeSeries
{
    internal class ShardedAdminTimeSeriesHandlerProcessorForDeleteTimeSeriesPolicy : AbstractAdminTimeSeriesHandlerProcessorForDeleteTimeSeriesPolicy<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedAdminTimeSeriesHandlerProcessorForDeleteTimeSeriesPolicy([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
    }
}
