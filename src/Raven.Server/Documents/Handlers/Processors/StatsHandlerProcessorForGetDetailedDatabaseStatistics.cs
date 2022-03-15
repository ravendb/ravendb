using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors
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

                return ValueTask.FromResult(stats);
            }
        }

        protected override Task<DetailedDatabaseStatistics> GetResultForRemoteNodeAsync(RavenCommand<DetailedDatabaseStatistics> command) => 
            RequestHandler.ExecuteRemoteAsync(command);
    }
}
