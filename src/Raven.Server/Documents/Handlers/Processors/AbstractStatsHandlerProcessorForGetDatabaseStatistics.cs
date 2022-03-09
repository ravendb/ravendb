using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.Json;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors
{
    internal abstract class AbstractStatsHandlerProcessorForGetDatabaseStatistics<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        protected AbstractStatsHandlerProcessorForGetDatabaseStatistics([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> operationContext) : base(requestHandler, operationContext)
        {
        }

        protected abstract ValueTask<DatabaseStatistics> GetDatabaseStatisticsAsync();

        public override async ValueTask ExecuteAsync()
        {
            var databaseStats = await GetDatabaseStatisticsAsync();

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                writer.WriteDatabaseStatistics(context, databaseStats);

        }
    }
}
