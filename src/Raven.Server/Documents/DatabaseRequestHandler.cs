using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.Indexes;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.Web;

namespace Raven.Server.Documents
{
    public abstract class DatabaseRequestHandler : RequestHandler
    {
        protected ContextPool ContextPool;
        protected DocumentsStorage DocumentsStorage;
        protected IndexStore IndexStore;

        public override void Init(RequestHandlerContext context)
        {
            base.Init(context);

            DocumentsStorage = context.Database.DocumentStorage;
            ContextPool = DocumentsStorage?.ContextPool;
            IndexStore = context.Database.IndexStore;
        }

        protected async Task WriteDocumentsAsync(RavenOperationContext context, IEnumerable<Document> documents)
        {
            var writer = new BlittableJsonTextWriter(context, ResponseBodyStream());
            await WriteDocumentsAsync(context, writer, documents);
            writer.Flush();
        }

        public static async Task WriteDocumentsAsync(RavenOperationContext context, BlittableJsonTextWriter writer, IEnumerable<Document> documents)
        {
            writer.WriteStartArray();

            bool first = true;
            foreach (var document in documents)
            {
                if (document == null)
                    continue;
                if (first == false)
                    writer.WriteComma();
                first = false;
                document.EnsureMetadata();
                await context.WriteAsync(writer, document.Data);
            }

            writer.WriteEndArray();
        }
    }
}