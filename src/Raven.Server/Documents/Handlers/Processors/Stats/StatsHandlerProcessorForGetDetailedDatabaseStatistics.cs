using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Stats
{
    internal class StatsHandlerProcessorForGetDetailedDatabaseStatistics : AbstractStatsHandlerProcessorForGetDetailedDatabaseStatistics<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public StatsHandlerProcessorForGetDetailedDatabaseStatistics([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override bool SupportsCurrentNode => true;

        protected override ValueTask<DetailedDatabaseStatistics> GetResultForCurrentNodeAsync()
        {
            using (var context = QueryOperationContext.Allocate(RequestHandler.Database, needsServerContext: true))
            using (context.OpenReadTransaction())
            {
                var stats = new DetailedDatabaseStatistics();

                StatsHandlerProcessorForGetDatabaseStatistics.FillDatabaseStatistics(stats, context, RequestHandler.Database);

                stats.CountOfTimeSeriesDeletedRanges = RequestHandler.Database.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesDeletedRanges(context.Documents);
                stats.CountOfIdentities = RequestHandler.ServerStore.Cluster.GetNumberOfIdentities(context.Server, RequestHandler.Database.Name);
                stats.CountOfCompareExchange = RequestHandler.ServerStore.Cluster.GetNumberOfCompareExchange(context.Server, RequestHandler.Database.Name);
                stats.CountOfCompareExchangeTombstones = RequestHandler.ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(context.Server, RequestHandler.Database.Name);

                return ValueTask.FromResult(stats);
            }
        }

        protected override Task<DetailedDatabaseStatistics> GetResultForRemoteNodeAsync(RavenCommand<DetailedDatabaseStatistics> command) => 
            RequestHandler.ExecuteRemoteAsync(command);
    }
}
