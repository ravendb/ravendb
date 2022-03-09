using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Handlers.Processors
{
    internal class StatsHandlerProcessorForGetDetailedDatabaseStatistics : AbstractStatsHandlerProcessorForGetDetailedDatabaseStatistics<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public StatsHandlerProcessorForGetDetailedDatabaseStatistics([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override string GetDatabaseName()
        {
            return ShardHelper.ToDatabaseName(RequestHandler.Database.Name);
        }

        protected override Task<DetailedDatabaseStatistics> GetDatabaseStatistics()
        {
            using (var context = QueryOperationContext.Allocate(RequestHandler.Database, needsServerContext: true))
            using (context.OpenReadTransaction())
            {
                var stats = new DetailedDatabaseStatistics();

                FillDatabaseStatistics(stats, context);

                stats.CountOfTimeSeriesDeletedRanges = RequestHandler.Database.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesDeletedRanges(context.Documents);
                
                return Task.FromResult(stats);
            }
        }
    }
}
