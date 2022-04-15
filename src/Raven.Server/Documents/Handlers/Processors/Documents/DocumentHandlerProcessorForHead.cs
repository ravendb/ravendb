using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Documents;

internal class DocumentHandlerProcessorForHead : AbstractDocumentHandlerProcessorForHead<DocumentHandler, DocumentsOperationContext>
{
    public DocumentHandlerProcessorForHead([NotNull] DocumentHandler requestHandler, [NotNull] JsonContextPoolBase<DocumentsOperationContext> contextPool) : base(requestHandler, contextPool)
    {
    }

    protected override ValueTask<(HttpStatusCode StatusCode, string ChangeVector)> GetStatusCodeAndChangeVectorAsync(string docId, DocumentsOperationContext context)
    {
        var changeVector = RequestHandler.GetStringFromHeaders(Constants.Headers.IfNoneMatch);

        using (context.OpenReadTransaction())
        {
            var document = RequestHandler.Database.DocumentsStorage.Get(context, docId, DocumentFields.ChangeVector);
            if (document == null)
                return new ValueTask<(HttpStatusCode StatusCode, string ChangeVector)>((HttpStatusCode.NotFound, null));
            
            var statusCode = HttpStatusCode.OK;
            string changeVectorToReturn = null;

            if (changeVector == document.ChangeVector)
                statusCode = HttpStatusCode.NotModified;
            else
                changeVectorToReturn = "\"" + document.ChangeVector + "\"";

            return new ValueTask<(HttpStatusCode StatusCode, string ChangeVector)>((statusCode, changeVectorToReturn));

        }
    }
}
