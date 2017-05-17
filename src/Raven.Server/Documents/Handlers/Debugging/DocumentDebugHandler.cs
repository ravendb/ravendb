using System.Globalization;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class DocumentDebugHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/documents/huge", "GET")]
        public Task HugeDocuments()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            using (context.OpenReadTransaction())
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Results");

                writer.WriteStartArray();

                var isFirst = true;

                foreach (var pair in context.DocumentDatabase.HugeDocuments.GetHugeDocuments())
                {
                    if (isFirst == false)
                        writer.WriteComma();

                    isFirst = false;

                    writer.WriteStartObject();

                    writer.WritePropertyName("Id");
                    writer.WriteString(pair.Key.Item1);

                    writer.WriteComma();

                    writer.WritePropertyName("Size");
                    writer.WriteInteger(pair.Value);

                    writer.WriteComma();

                    writer.WritePropertyName("LastAccess");
                    writer.WriteString(pair.Key.Item2.ToString("o", CultureInfo.InvariantCulture));

                    writer.WriteEndObject();
                }

                writer.WriteEndArray();

                writer.WriteEndObject();
            }

            return Task.CompletedTask;
        }
    }
}