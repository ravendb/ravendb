using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Exceptions.Transformers;
using Raven.Client.Documents.Transformers;
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
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), name);
                var transformerDefinition = JsonDeserializationServer.TransformerDefinition(json);
                transformerDefinition.Name = name;

                var transformerId = Database.TransformerStore.CreateTransformer(transformerDefinition);

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName("Transformer");
                    writer.WriteString(name);
                    writer.WriteComma();

                    writer.WritePropertyName("TransformerId");
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
            var namesOnly = GetBoolValueQueryString("namesOnly", required: false) ?? false;

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

                writer.WriteStartObject();
                writer.WriteResults(context, transformerDefinitions, (w, c, definition) =>
                {
                    if (namesOnly)
                    {
                        w.WriteString(definition.Name);
                        return;
                    }

                    w.WriteTransformerDefinition(c, definition);
                });

                writer.WriteEndObject();
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/transformers/rename", "POST")]
        public Task Rename()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var newName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("newName");

            Database.TransformerStore.Rename(name, newName);

            return NoContent();
        }

        [RavenAction("/databases/*/transformers/set-lock", "POST")]
        public Task SetLockMode()
        {
            var names = GetStringValuesQueryString("name");
            var modeStr = GetQueryStringValueAndAssertIfSingleAndNotEmpty("mode");

            TransformerLockMode mode;
            if (Enum.TryParse(modeStr, out mode) == false)
                throw new InvalidOperationException("Query string value 'mode' is not a valid mode: " + modeStr);

            foreach (var name in names)
            {
                var transformer = Database.TransformerStore.GetTransformer(name);
                if (transformer == null)
                    TransformerDoesNotExistException.ThrowFor(name);

                transformer.SetLock(mode);
            }

            return NoContent();
        }

        [RavenAction("/databases/*/transformers", "DELETE")]
        public Task Delete()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            Database.TransformerStore.DeleteTransformer(name);

            return NoContent();
        }
    }
}