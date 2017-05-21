using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Documents.Operations;
using Raven.Server.Json;
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
                    writer.WriteStartObject();
                    writer.WritePropertyName("Id");
                    writer.WriteInteger(nextId);
                    writer.WriteEndObject();                   
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/operations/kill", "POST")]
        public Task Kill()
        {
            var id = GetLongQueryString("id");
            // ReSharper disable once PossibleInvalidOperationException
            Database.Operations.KillOperation(id.Value);

            return NoContent();
        }

        [RavenAction("/databases/*/operations", "GET")]
        public Task GetAll()
        {
            var id = GetLongQueryString("id", required: false);

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                IEnumerable<DatabaseOperations.Operation> operations;
                if (id.HasValue == false)
                    operations = Database.Operations.GetAll();
                else
                {
                    var operation = Database.Operations.GetOperation(id.Value);
                    if (operation == null)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return Task.CompletedTask;
                    }

                    operations = new List<DatabaseOperations.Operation> { operation };
                }

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WriteArray(context, "Results", operations, (w, c, operation) =>
                    {
                        c.Write(w, operation.ToJson());
                    });
                    writer.WriteEndObject();
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/operations/state", "GET")]
        public Task State()
        {
            var id = GetLongQueryString("id");
            // ReSharper disable once PossibleInvalidOperationException
            var state = Database.Operations.GetOperation(id.Value)?.State;

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
                    context.Write(writer, state.ToJson());
                }
            }

            return Task.CompletedTask;
        }

    }
}