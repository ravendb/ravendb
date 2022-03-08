using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Studio;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors
{
    internal abstract class AbstractStatsHandlerProcessorForGetStudioFooterStats<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        public AbstractStatsHandlerProcessorForGetStudioFooterStats([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
        {
        }

        protected abstract ValueTask<FooterStatistics> GetFooterStatistics();

        public override async ValueTask ExecuteAsync()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                var stats = await GetFooterStatistics();

                writer.WriteStartObject();

                writer.WritePropertyName(nameof(FooterStatistics.CountOfDocuments));
                writer.WriteInteger(stats.CountOfDocuments);
                writer.WriteComma();
                
                writer.WritePropertyName(nameof(FooterStatistics.CountOfIndexes));
                writer.WriteInteger(stats.CountOfIndexes);

                if (stats.CountOfStaleIndexes != null)
                {
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(FooterStatistics.CountOfStaleIndexes));
                    writer.WriteInteger(stats.CountOfStaleIndexes ?? -1);
                }

                if (stats.CountOfIndexingErrors != null)
                {
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(FooterStatistics.CountOfIndexingErrors));
                    writer.WriteInteger(stats.CountOfIndexingErrors ?? -1);
                }

                writer.WriteEndObject();
            }
        }
    }
}
