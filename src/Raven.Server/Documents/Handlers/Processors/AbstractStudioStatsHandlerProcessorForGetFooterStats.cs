using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Studio;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors
{
    internal abstract class AbstractStudioStatsHandlerProcessorForGetFooterStats<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        protected AbstractStudioStatsHandlerProcessorForGetFooterStats([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
            : base(requestHandler, contextPool)
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
