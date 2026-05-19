using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Studio;
using Raven.Server.Documents.Studio;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Studio
{
    internal abstract class AbstractStudioStatsHandlerProcessorForGetFooterStats<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<FooterStatistics, TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractStudioStatsHandlerProcessorForGetFooterStats([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override bool SupportsCurrentNode => true;

        protected abstract ValueTask<FooterStatistics> GetFooterStatisticsAsync();

        protected override RavenCommand<FooterStatistics> CreateCommandForNode(string nodeTag) => new GetStudioFooterStatisticsOperation.GetStudioFooterStatisticsCommand(nodeTag);

        protected override async ValueTask HandleCurrentNodeAsync()
        {
            var stats = await GetFooterStatisticsAsync();

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName(nameof(FooterStatistics.CountOfDocuments));
                writer.WriteInteger(stats.CountOfDocuments);
                writer.WriteComma();

                writer.WritePropertyName(nameof(FooterStatistics.CountOfIndexes));
                writer.WriteInteger(stats.CountOfIndexes);
                writer.WriteComma();

                writer.WriteArray(nameof(FooterStatistics.StaleIndexes), stats.StaleIndexes);
                writer.WriteComma();

                writer.WritePropertyName(nameof(FooterStatistics.CountOfStaleIndexes));
                writer.WriteInteger(stats.CountOfStaleIndexes);
                writer.WriteComma();

                writer.WritePropertyName(nameof(FooterStatistics.CountOfIndexingErrors));
                writer.WriteInteger(stats.CountOfIndexingErrors);
                writer.WriteComma();

                writer.WritePropertyName(nameof(FooterStatistics.CountOfEtlTasksErrors));
                writer.WriteInteger(stats.CountOfEtlTasksErrors);
                writer.WriteComma();

                writer.WritePropertyName(nameof(FooterStatistics.CountOfAiTasksErrors));
                writer.WriteInteger(stats.CountOfAiTasksErrors);

                writer.WriteEndObject();
            }
        }
    }
}
