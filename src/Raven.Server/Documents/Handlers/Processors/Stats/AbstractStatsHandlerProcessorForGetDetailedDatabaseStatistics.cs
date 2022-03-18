using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Server.Json;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Stats
{
    internal abstract class AbstractStatsHandlerProcessorForGetDetailedDatabaseStatistics<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<DetailedDatabaseStatistics, TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {

        protected AbstractStatsHandlerProcessorForGetDetailedDatabaseStatistics([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> operationContext) : base(requestHandler, operationContext)
        {
        }

        protected override RavenCommand<DetailedDatabaseStatistics> CreateCommandForNode(string nodeTag)
        {
            return new GetDetailedStatisticsOperation.GetDetailedStatisticsCommand(debugTag: null, nodeTag);
        }

        protected override async ValueTask WriteResultAsync(DetailedDatabaseStatistics result)
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                writer.WriteDetailedDatabaseStatistics(context, result);
        }
    }
}
