using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Documents;

internal class DocumentHandlerProcessorForHead : AbstractDocumentHandlerProcessorForHead<DocumentHandler, DocumentsOperationContext>
{
    public DocumentHandlerProcessorForHead([NotNull] DocumentHandler requestHandler) : base(requestHandler)
    {
    }

    protected override ValueTask HandleHeadRequest(string docId, string changeVector)
    {
        using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var document = RequestHandler.Database.DocumentsStorage.Get(context, docId, DocumentFields.ChangeVector);
            if (document == null)
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            else
            {
                if (changeVector == document.ChangeVector)
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                else
                    HttpContext.Response.Headers[Constants.Headers.Etag] = "\"" + document.ChangeVector + "\"";
            }
        }

        return ValueTask.CompletedTask;
    }
}
