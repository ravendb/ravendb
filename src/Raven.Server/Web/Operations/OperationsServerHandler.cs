using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Sparrow.Json;

namespace Raven.Server.Web.Operations
{
    public class OperationsServerHandler : ServerRequestHandler
    {
        [RavenAction("/admin/operations/next-operation-id", "GET", AuthorizationStatus.Operator)]
        public async Task GetNextOperationId()
        {
            var nextId = ServerStore.Operations.GetNextOperationId();

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Id");
                    writer.WriteInteger(nextId);
                    writer.WriteEndObject();
                }
            }
        }

        [RavenAction("/admin/operations/kill", "POST", AuthorizationStatus.Operator)]
        public async Task Kill()
        {
            var id = GetLongQueryString("id");
            // ReSharper disable once PossibleInvalidOperationException
            await ServerStore.Operations.KillOperationAsync(id, CancellationToken.None);

            NoContentStatus();
        }

        [RavenAction("/operations/state", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task State()
        {
            var id = GetLongQueryString("id");
            // ReSharper disable once PossibleInvalidOperationException
            var operation = ServerStore.Operations.GetOperation(id);
            if (operation == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return;
            }

            if (operation.DatabaseName == null) // server level op
            {
                if (await IsOperatorAsync() == false)
                    return;
            }
            else if (await CanAccessDatabaseAsync(operation.DatabaseName, requireAdmin: false, requireWrite: false) == false)
            {
                return;
            }

            var state = operation.State;

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, state.ToJson());
                }
            }
        }
    }
}
