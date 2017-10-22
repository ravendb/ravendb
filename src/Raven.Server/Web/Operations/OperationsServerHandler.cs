using System.Net;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Sparrow.Json;

namespace Raven.Server.Web.Operations
{
    public class OperationsServerHandler : RequestHandler
    {
        [RavenAction("/admin/operations/next-operation-id", "GET", AuthorizationStatus.Operator)]
        public Task GetNextOperationId()
        {
            var nextId = ServerStore.Operations.GetNextOperationId();

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
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

        [RavenAction("/admin/operations/kill", "POST", AuthorizationStatus.Operator)]
        public Task Kill()
        {
            var id = GetLongQueryString("id");
            // ReSharper disable once PossibleInvalidOperationException
            ServerStore.Operations.KillOperation(id);

            return NoContent();
        }

        [RavenAction("/operations/state", "GET", AuthorizationStatus.ValidUser)]
        public Task State()
        {
            var id = GetLongQueryString("id");
            // ReSharper disable once PossibleInvalidOperationException
            var operation = ServerStore.Operations.GetOperation(id);
            if (operation == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return Task.CompletedTask;
            }

            if (operation.Database == null) // server level op
            {
                if (IsOperator() == false)
                    return Task.CompletedTask;
            }
            else if (TryGetAllowedDbs(operation.Database.Name, out var _, requireAdmin: false) == false)
            {
                return Task.CompletedTask;
            }

            var state = operation.State;



            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
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
