using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Server.Json;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors
{
    internal abstract class AbstractIndexHandlerProcessorForGetDatabaseIndexStatistics<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        protected AbstractIndexHandlerProcessorForGetDatabaseIndexStatistics([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> operationContext) : base(requestHandler, operationContext)
        {
        }

        protected abstract ValueTask<IndexStats[]> GetDatabaseIndexStatisticsAsync();

        public override async ValueTask ExecuteAsync()
        {
            var indexesStats = await GetDatabaseIndexStatisticsAsync();

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                writer.WriteIndexesStats(context, indexesStats);

        }
    }
}
