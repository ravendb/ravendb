using System.Net;
using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class AdminIndexHandler : AdminDatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/indexes/compact", "POST")]
        public Task Compact()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            var index = Database.IndexStore.GetIndex(name);
            if (index == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return Task.CompletedTask;
            }

            var token = CreateOperationToken();
            var operationId = Database.Operations.GetNextOperationId();

            Database.Operations.AddOperation(
                "Compact index: " + index.Name,
                DatabaseOperations.PendingOperationType.IndexCompact,
                onProgress => Task.Factory.StartNew(() => index.Compact(onProgress), token.Token), operationId, token);

            JsonOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteOperationId(context, operationId);
            }

            return Task.CompletedTask;
        }
    }
}