using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Commands.Batches;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Documents;

internal class DocumentHandlerProcessorForPut : AbstractDocumentHandlerProcessorForPut<DocumentHandler, DocumentsOperationContext>
{
    public DocumentHandlerProcessorForPut([NotNull] DocumentHandler requestHandler) : base(requestHandler)
    {
    }

    protected override async ValueTask HandleDocumentPutAsync(string id, string changeVector, BlittableJsonReaderObject doc, DocumentsOperationContext context)
    {
        var changeVectorLsv = context.GetLazyString(changeVector);

        using (var cmd = new MergedPutCommand(doc, id, changeVectorLsv, RequestHandler.Database, shouldValidateAttachments: true))
        {
            await RequestHandler.Database.TxMerger.Enqueue(cmd);

            HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName(nameof(PutResult.Id));
                writer.WriteString(cmd.PutResult.Id);
                writer.WriteComma();

                writer.WritePropertyName(nameof(PutResult.ChangeVector));
                writer.WriteString(cmd.PutResult.ChangeVector);

                writer.WriteEndObject();
            }
        }
    }
}
