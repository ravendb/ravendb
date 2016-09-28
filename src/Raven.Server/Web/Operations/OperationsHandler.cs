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
            var nextId = Database.Operations.GetNextOperationId();

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

        [RavenAction("/databases/*/operation/kill", "POST")]
        public Task OperationKill()
        {
            var id = GetLongQueryString("id");
            // ReSharper disable once PossibleInvalidOperationException
            Database.Operations.KillOperation(id.Value);

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/operation/dismiss", "GET")]
        public Task OperationDismiss()
        {
            var id = GetLongQueryString("id");
            // ReSharper disable once PossibleInvalidOperationException
            Database.Operations.DismissOperation(id.Value);

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/operations", "GET")]
        public Task OperationsGet()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartArray();

                    var first = true;

                    foreach (var operation in Database.Operations.GetAll())
                    {
                        if (first == false)
                            writer.WriteComma();
                        first = false;

                        writer.WriteObject(context.ReadObject(operation.ToJson(), "operation"));
                    }

                    writer.WriteEndArray();
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/operation", "GET")]
        public Task Operation()
        {
            var id = GetLongQueryString("id");
            // ReSharper disable once PossibleInvalidOperationException
            var operation = Database.Operations.GetOperation(id.Value);

            if (operation == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return Task.CompletedTask;
            }

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteObject(context.ReadObject(operation.ToJson(), "operation"));
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/operations/status", "GET")]
        public Task Operations()
        {
            var id = GetLongQueryString("id");
            // ReSharper disable once PossibleInvalidOperationException
            var state = Database.Operations.GetOperationState(id.Value);

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