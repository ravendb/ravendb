using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.TransactionCommands;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Documents;

internal class DocumentHandlerProcessorForDelete : AbstractDocumentHandlerProcessorForDelete<DocumentHandler, DocumentsOperationContext>
{
    public DocumentHandlerProcessorForDelete([NotNull] DocumentHandler requestHandler) : base(requestHandler)
    {
    }

    protected override async ValueTask HandleDeleteDocument(string docId, string changeVector)
    {
        using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var changeVectorLsv = context.GetLazyString(changeVector);

            var cmd = new DeleteDocumentCommand(docId, changeVectorLsv, RequestHandler.Database);
            await RequestHandler.Database.TxMerger.Enqueue(cmd);
        }

        RequestHandler.NoContentStatus();
    }
}
