using System.Collections.Generic;

using Raven.Server.Documents.Indexes;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;

namespace Raven.Server.Documents
{
    public abstract class DatabaseRequestHandler : RequestHandler
    {
        protected DocumentsContextPool ContextPool;
        protected DocumentDatabase Database;
        protected IndexStore IndexStore;

        public override void Init(RequestHandlerContext context)
        {
            base.Init(context);

            Database = context.Database;
            ContextPool = Database?.DocumentsStorage?.ContextPool;
            IndexStore = context.Database.IndexStore;
        }

        protected void  WriteDocuments(MemoryOperationContext context, IEnumerable<Document> documents)
        {
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                WriteDocuments(context, writer, documents);
        }

        public static void WriteDocuments(MemoryOperationContext context, BlittableJsonTextWriter writer, 
            IEnumerable<Document> documents)
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
                context.Write(writer, document.Data);
            }

            writer.WriteEndArray();
        }

        public static void WriteDocuments(MemoryOperationContext context, BlittableJsonTextWriter writer,
            List<Document> documents, int start, int count)
        {
            writer.WriteStartArray();

            bool first = true;
            for (int index = start; index < count; index++)
            {
                var document = documents[index];
                if (document == null)
                    continue;
                if (first == false)
                    writer.WriteComma();
                first = false;
                document.EnsureMetadata();
                context.Write(writer, document.Data);
            }

            writer.WriteEndArray();
        }
    }
}