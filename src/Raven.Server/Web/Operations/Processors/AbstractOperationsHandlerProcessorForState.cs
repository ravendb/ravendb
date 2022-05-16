using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.TrafficWatch;
using Sparrow.Json;

namespace Raven.Server.Web.Operations.Processors;

internal abstract class AbstractOperationsHandlerProcessorForState<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractOperationsHandlerProcessorForState([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract OperationState GetOperationState(long operationId);

    public override async ValueTask ExecuteAsync()
    {
        var id = RequestHandler.GetLongQueryString("id");

        var state = GetOperationState(id);

        if (state == null)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            return;
        }

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            context.Write(writer, state.ToJson());

            // writes Patch response
            if (TrafficWatchManager.HasRegisteredClients)
                RequestHandler.AddStringToHttpContext(writer.ToString(), TrafficWatchChangeType.Operations);
        }
    }
}
