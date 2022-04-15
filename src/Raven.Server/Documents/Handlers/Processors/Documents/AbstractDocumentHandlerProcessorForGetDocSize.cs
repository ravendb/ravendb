using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Documents;

internal abstract class AbstractDocumentHandlerProcessorForGetDocSize<TDocSize, TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractDocumentHandlerProcessorForGetDocSize([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
    {
    }

    protected abstract void WriteDocSize(TDocSize size, TOperationContext context, AsyncBlittableJsonTextWriter writer);

    protected abstract ValueTask<(HttpStatusCode StatusCode, TDocSize SizeResult)> GetResultAndStatusCodeAsync(string docId, TOperationContext context);

    public override async ValueTask ExecuteAsync()
    {
        var id = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");

        using (ContextPool.AllocateOperationContext(out TOperationContext context))
        {
            var result = await GetResultAndStatusCodeAsync(id, context);

            HttpContext.Response.StatusCode = (int)result.StatusCode;

            if (result.SizeResult != null)
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    WriteDocSize(result.SizeResult, context, writer);
                }
            }
        }
    }
}
