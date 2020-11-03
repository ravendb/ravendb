using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Studio;
using Raven.Server.Routing;
using Sparrow.Json;

namespace Raven.Server.Web.Studio
{
    public class StudioStatsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/studio/footer/stats", "GET", AuthorizationStatus.ValidUser)]
        public async Task FooterStats()
        {
            using (var context = QueryOperationContext.Allocate(Database, needsServerContext: true))
            await using (var writer = new AsyncBlittableJsonTextWriter(context.Documents, ResponseBodyStream()))
            using (context.OpenReadTransaction())
            {
                var indexes = Database.IndexStore.GetIndexes().ToList();

                writer.WriteStartObject();

                writer.WritePropertyName(nameof(FooterStatistics.CountOfDocuments));
                writer.WriteInteger(Database.DocumentsStorage.GetNumberOfDocuments(context.Documents));
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
        }
    }
}
