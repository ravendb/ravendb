using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Indexing;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class TransformerHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/transformers", "PUT")]
        public async Task Put()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var json = await context.ReadForDiskAsync(RequestBodyStream(), name);
                var transformerDefinition = JsonDeserialization.TransformerDefinition(json);
                transformerDefinition.Name = name;

                var transformerId = Database.TransformerStore.CreateTransformer(transformerDefinition);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName(context.GetLazyString("Transformer"));
                    writer.WriteString(context.GetLazyString(name));
                    writer.WriteComma();

                    writer.WritePropertyName(context.GetLazyString("TransformerId"));
                    writer.WriteInteger(transformerId);

                    writer.WriteEndObject();
                }
            }
        }

        [RavenAction("/databases/*/transformers", "GET")]
        public Task GetAll()
        {
            var name = GetStringQueryString("name", required: false);

            var start = GetStart();
            var pageSize = GetPageSize();

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                TransformerDefinition[] transformerDefinitions;
                if (string.IsNullOrEmpty(name))
                    transformerDefinitions = Database.TransformerStore
                        .GetTransformers()
                        .OrderBy(x => x.Name)
                        .Skip(start)
                        .Take(pageSize)
                        .Select(x => x.Definition)
                        .ToArray();
                else
                {
                    var transformer = Database.TransformerStore.GetTransformer(name);
                    if (transformer == null)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return Task.CompletedTask;
                    }

                    transformerDefinitions = new[] { transformer.Definition };
                }

                writer.WriteStartArray();

                var isFirst = true;
                foreach (var transformerDefinition in transformerDefinitions)
                {
                    if (isFirst == false)
                        writer.WriteComma();

                    isFirst = false;

                    writer.WriteTransformerDefinition(context, transformerDefinition);
                }

                writer.WriteEndArray();
            }

            return Task.CompletedTask;
        }
    }
}