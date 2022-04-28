using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Stats
{
    internal abstract class AbstractStatsHandlerProcessorForGetDetailedDatabaseStatistics<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<DetailedDatabaseStatistics, TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext 
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {

        protected AbstractStatsHandlerProcessorForGetDetailedDatabaseStatistics([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override RavenCommand<DetailedDatabaseStatistics> CreateCommandForNode(string nodeTag) => new GetDetailedStatisticsOperation.GetDetailedStatisticsCommand(debugTag: null, nodeTag);
    }
}
