using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors
{
    internal abstract class AbstractStatsHandlerProcessorForGetDetailedDatabaseStatistics<TRequestHandler, TOperationContext> : AbstractStatsHandler<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {

        protected AbstractStatsHandlerProcessorForGetDetailedDatabaseStatistics([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> operationContext) : base(requestHandler, operationContext)
        {
        }

        protected abstract string GetDatabaseName();

        protected abstract Task<DetailedDatabaseStatistics> GetDatabaseStatistics();

        public override async ValueTask ExecuteAsync()
        {
            var databaseName = GetDatabaseName();
            var databaseStats = await GetDatabaseStatistics();

            GetDetailedDatabaseStatistics(databaseStats, databaseName);

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                writer.WriteDetailedDatabaseStatistics(context, databaseStats);
        }

        public void GetDetailedDatabaseStatistics(DetailedDatabaseStatistics stats, string databaseName)
        {
            using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
            using (serverContext.OpenReadTransaction())
            {
                stats.CountOfIdentities = RequestHandler.ServerStore.Cluster.GetNumberOfIdentities(serverContext, databaseName);
                stats.CountOfCompareExchange = RequestHandler.ServerStore.Cluster.GetNumberOfCompareExchange(serverContext, databaseName);
                stats.CountOfCompareExchangeTombstones = RequestHandler.ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(serverContext, databaseName);
            }
        }
    }
}
