using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments;

internal abstract class AbstractAttachmentHandlerProcessorForHeadAttachment<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext 
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractAttachmentHandlerProcessorForHeadAttachment([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract ValueTask HandleHeadAttachmentAsync(string documentId, string name, string changeVector);

    public override ValueTask ExecuteAsync()
    {
        var documentId = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
        var name = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
        var changeVector = RequestHandler.GetStringFromHeaders(Constants.Headers.IfNoneMatch);

        return HandleHeadAttachmentAsync(documentId, name, changeVector);
    }
}
