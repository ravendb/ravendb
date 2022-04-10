using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Server.Json;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Stats
{
    internal abstract class AbstractStatsHandlerProcessorForGetDatabaseStatistics<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<DatabaseStatistics, TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        protected AbstractStatsHandlerProcessorForGetDatabaseStatistics([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> operationContext) : base(requestHandler, operationContext)
        {
        }

        protected override RavenCommand<DatabaseStatistics> CreateCommandForNode(string nodeTag) => new GetStatisticsOperation.GetStatisticsCommand(debugTag: null, nodeTag);
    }
}
