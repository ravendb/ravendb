using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Indexes;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Stats
{
    internal sealed class StatsHandlerProcessorForGetDetailedDatabaseStatistics : AbstractStatsHandlerProcessorForGetDetailedDatabaseStatistics<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public StatsHandlerProcessorForGetDetailedDatabaseStatistics([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override bool SupportsCurrentNode => true;

        protected override ValueTask HandleCurrentNodeAsync()
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
                //TODO: egor stats.CountOfRetiredAttachments = RequestHandler.Database.DocumentsStorage.AttachmentsStorage.GetNumberOfRetiredAttachments(context.Documents);
                return WriteResultAsync(stats);
            }
        }

        protected override Task HandleRemoteNodeAsync(ProxyCommand<DetailedDatabaseStatistics> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);

        private async ValueTask WriteResultAsync(DetailedDatabaseStatistics result)
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                writer.WriteDetailedDatabaseStatistics(context, result);
        }
    }
}
