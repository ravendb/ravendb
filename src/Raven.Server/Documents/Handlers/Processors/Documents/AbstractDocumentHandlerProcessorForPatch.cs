using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Documents;

internal abstract class AbstractDocumentHandlerProcessorForPatch<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    where TOperationContext : JsonOperationContext
{
    protected AbstractDocumentHandlerProcessorForPatch([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        var id = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
        var isTest = RequestHandler.GetBoolValueQueryString("test", required: false) ?? false;
        var debugMode = RequestHandler.GetBoolValueQueryString("debug", required: false) ?? isTest;
        var skipPatchIfChangeVectorMismatch = RequestHandler.GetBoolValueQueryString("skipPatchIfChangeVectorMismatch", required: false) ?? false;
        var changeVector = RequestHandler.GetStringFromHeaders("If-Match");

        using (ContextPool.AllocateOperationContext(out TOperationContext context))
        {
            var request = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "ScriptedPatchRequest");
            if (request.TryGet("Patch", out BlittableJsonReaderObject patchCmd) == false || patchCmd == null)
                throw new ArgumentException("The 'Patch' field in the body request is mandatory");

            await HandleDocumentPatchAsync(id, changeVector, request, skipPatchIfChangeVectorMismatch, debugMode, isTest, context);
        }
    }

    protected abstract ValueTask HandleDocumentPatchAsync(string id, string changeVector, BlittableJsonReaderObject patchRequest, bool skipPatchIfChangeVectorMismatch, bool debugMode, bool isTest, TOperationContext context);
}
