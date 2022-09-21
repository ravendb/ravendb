using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Studio;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Studio
{
    internal abstract class AbstractStudioStatsHandlerProcessorForGetFooterStats<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext 
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractStudioStatsHandlerProcessorForGetFooterStats([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract ValueTask<FooterStatistics> GetFooterStatisticsAsync();

        public override async ValueTask ExecuteAsync()
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

                writer.WriteArray(nameof(FooterStatistics.StaleIndexes), stats.StaleIndexes);

                writer.WriteComma();
                writer.WritePropertyName(nameof(FooterStatistics.CountOfStaleIndexes));
                writer.WriteInteger(stats.CountOfStaleIndexes);

                writer.WriteComma();
                writer.WritePropertyName(nameof(FooterStatistics.CountOfIndexingErrors));
                writer.WriteInteger(stats.CountOfIndexingErrors);

                writer.WriteEndObject();
            }
        }
    }
}
