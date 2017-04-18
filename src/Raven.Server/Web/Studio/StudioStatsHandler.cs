using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Documents.Studio;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Web.Studio
{
    public class StudioStatsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/studio/footer/stats", "GET")]
        public Task FooterStats()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            using (context.OpenReadTransaction())
            {
                var indexes = Database.IndexStore.GetIndexes().ToList();

                writer.WriteStartObject();

                writer.WritePropertyName(nameof(FooterStatistics.CountOfDocuments));
                writer.WriteInteger(Database.DocumentsStorage.GetNumberOfDocuments(context));
                writer.WriteComma();


                writer.WritePropertyName(nameof(FooterStatistics.CountOfIndexes));
                writer.WriteInteger(indexes.Count);
                writer.WriteComma();

                writer.WritePropertyName(nameof(FooterStatistics.CountOfStaleIndexes));
                writer.WriteInteger(indexes.Count(i => i.IsStale(context)));
                writer.WriteComma();

                writer.WritePropertyName(nameof(FooterStatistics.CountOfIndexingErrors));
                writer.WriteInteger(indexes.Sum(index => index.GetErrorCount()));

                writer.WriteEndObject();
            }


            return Task.CompletedTask;
        }
    }
}