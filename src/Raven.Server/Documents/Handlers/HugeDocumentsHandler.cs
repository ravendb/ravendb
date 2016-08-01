using System.Globalization;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class HugeDocumentsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/huge-documents", "GET")]
        public Task HugeDocuments()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            using (context.OpenReadTransaction())
            {
                writer.WriteStartArray();

                var isFirst = true;

                foreach (var pair in context.DocumentDatabase.HugeDocuments.GetHugeDocuments())
                {
                    if (isFirst == false)
                        writer.WriteComma();

                    isFirst = false;

                    writer.WriteStartObject();

                    writer.WritePropertyName(("Id"));
                    writer.WriteString((pair.Key.Item1));

                    writer.WriteComma();

                    writer.WritePropertyName(("Size"));
                    writer.WriteInteger(pair.Value);

                    writer.WriteComma();

                    writer.WritePropertyName(("Last Access"));
                    writer.WriteString(pair.Key.Item2.ToString("o", CultureInfo.InvariantCulture));

                    writer.WriteEndObject();

                }

                writer.WriteEndArray();
            }

            return Task.CompletedTask;
        }
    }
}