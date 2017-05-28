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

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), name);
                var transformerDefinition = JsonDeserializationServer.TransformerDefinition(json);
                transformerDefinition.Name = name;

                var etag = await Database.TransformerStore.CreateTransformer(transformerDefinition);
                await Database.WaitForIndexNotification(etag);

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName(nameof(PutTransformerResult.Transformer));
                    writer.WriteString(name);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(PutTransformerResult.Etag));
                    writer.WriteInteger(etag);

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
                writer.WriteArray(context, "Results", transformerDefinitions, (w, c, definition) =>
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
        public async Task Rename()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var newName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("newName");

            await Database.TransformerStore.Rename(name, newName);

            NoContentStatus();
        }

        [RavenAction("/databases/*/transformers/set-lock", "POST")]
        public async Task SetLockMode()
        {
            var names = GetStringValuesQueryString("name");
            var modeStr = GetQueryStringValueAndAssertIfSingleAndNotEmpty("mode");

            TransformerLockMode mode;
            if (Enum.TryParse(modeStr, out mode) == false)
                throw new InvalidOperationException("Query string value 'mode' is not a valid mode: " + modeStr);

            foreach (var name in names)
            {
                await Database.TransformerStore.SetLock(name, mode);
            }

            NoContentStatus();
        }

        [RavenAction("/databases/*/transformers", "DELETE")]
        public async Task Delete()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            await Database.TransformerStore.DeleteTransformer(name);

            NoContentStatus();
        }
    }
}