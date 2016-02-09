using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Server.Routing;

namespace Raven.Server.Documents
{
    public class CollectionsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/collections/stats", "GET")]
        public async Task GetCollectionStats()
        {
            RavenOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                context.Transaction = context.Environment.ReadTransaction();
                var collections= new DynamicJsonValue();
                var result = new DynamicJsonValue
                {
                    ["NumberOfDocuments"] = DocumentsStorage.GetNumberOfDocuments(context),
                    ["Collections"] = collections
                };

                foreach (var collectionStat in DocumentsStorage.GetCollections(context))
                {
                    collections[collectionStat.Name] = collectionStat.Count;
                }
                var writer = new BlittableJsonTextWriter(context, ResponseBodyStream());
                await context.WriteAsync(writer, result);
                writer.Flush();
            }
        }
    }
}