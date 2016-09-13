using System.Net;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Web.Operations
{
    public class OperationsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/operations/next-operation-id", "GET")]
        public Task GetNextOperationId()
        {
            var nextId = Database.DatabaseOperations.GetNextOperationId();

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteInteger(nextId);
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/operations/status", "GET")]
        public Task Operations()
        {
            var id = GetLongQueryString("id");
            // ReSharper disable once PossibleInvalidOperationException
            var state = Database.DatabaseOperations.GetOperationState(id.Value);

            if (state == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return Task.CompletedTask;
            }

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteObject(context.ReadObject(state.ToJson(), "operation"));
                }
            }
            
            return Task.CompletedTask;
        }

    }
}