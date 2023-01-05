using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Http;
using Raven.Server.Extensions;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes
{
    internal abstract class AbstractIndexHandlerProcessorForGetDatabaseIndexStatistics<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<IndexStats[], TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext 
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected bool ShouldCalculateStats => RequestHandler.HttpContext.Request.IsFromStudio() || RequestHandler.GetBoolValueQueryString(GetIndexesStatisticsOperation.GetIndexesStatisticsCommand.IncludeStatsParamName, required: false) == true;

        protected AbstractIndexHandlerProcessorForGetDatabaseIndexStatistics([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override RavenCommand<IndexStats[]> CreateCommandForNode(string nodeTag) => new GetIndexesStatisticsOperation.GetIndexesStatisticsCommand(nodeTag, ShouldCalculateStats);
    }
}
