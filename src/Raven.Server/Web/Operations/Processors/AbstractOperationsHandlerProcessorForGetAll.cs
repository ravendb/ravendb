using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Operations;
using Sparrow.Json;

namespace Raven.Server.Web.Operations.Processors;

internal abstract class AbstractOperationsHandlerProcessorForGetAll<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractOperationsHandlerProcessorForGetAll([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract AbstractOperation GetOperation(long operationId);

    protected abstract IEnumerable<AbstractOperation> GetAllOperations();

    public override async ValueTask ExecuteAsync()
    {
        var id = RequestHandler.GetLongQueryString("id", required: false);

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            IEnumerable<AbstractOperation> operations;
            if (id.HasValue == false)
                operations = GetAllOperations();
            else
            {
                var operation = GetOperation(id.Value);
                if (operation == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

                operations = new List<AbstractOperation> { operation };
            }

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WriteArray(context, "Results", operations, (w, c, operation) => c.Write(w, operation.ToJson()));
                writer.WriteEndObject();
            }
        }
    }
}
