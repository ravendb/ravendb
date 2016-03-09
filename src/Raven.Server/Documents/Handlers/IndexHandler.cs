using System;
using System.Net;
using System.Threading.Tasks;

using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers
{
    public class IndexHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/indexes", "RESET")]
        public Task Reset()
        {
            var names = HttpContext.Request.Query["name"];
            if (names.Count != 1)
                throw new ArgumentException("Query string value 'name' must appear exactly once");
            if (string.IsNullOrWhiteSpace(names[0]))
                throw new ArgumentException("Query string value 'name' must have a non empty value");

            var newIndexId = Database.IndexStore.ResetIndex(names[0]);

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(context.GetLazyString("IndexId"));
                writer.WriteInteger(newIndexId);
                writer.WriteEndObject();
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/indexes", "DELETE")]
        public Task Delete()
        {
            var names = HttpContext.Request.Query["name"];
            if (names.Count != 1)
                throw new ArgumentException("Query string value 'name' must appear exactly once");
            if (string.IsNullOrWhiteSpace(names[0]))
                throw new ArgumentException("Query string value 'name' must have a non empty value");

            Database.IndexStore.DeleteIndex(names[0]);

            HttpContext.Response.StatusCode = (int)HttpStatusCode.NoContent;
            return Task.CompletedTask;
        }
    }
}