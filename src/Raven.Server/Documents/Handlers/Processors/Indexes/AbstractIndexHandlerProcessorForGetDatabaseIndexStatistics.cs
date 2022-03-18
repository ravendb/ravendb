using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Http;
using Raven.Server.Json;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes
{
    internal abstract class AbstractIndexHandlerProcessorForGetDatabaseIndexStatistics<TRequestHandler, TOperationContext> : AbstractHandlerReadProcessor<IndexStats[], TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        protected AbstractIndexHandlerProcessorForGetDatabaseIndexStatistics([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> operationContext) : base(requestHandler, operationContext)
        {
        }

        protected override RavenCommand<IndexStats[]> CreateCommandForNode(string nodeTag) => new GetIndexesStatisticsOperation.GetIndexesStatisticsCommand(nodeTag);

        protected override async ValueTask WriteResultAsync(IndexStats[] result)
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                writer.WriteIndexesStats(context, result);
        }
    }
}
