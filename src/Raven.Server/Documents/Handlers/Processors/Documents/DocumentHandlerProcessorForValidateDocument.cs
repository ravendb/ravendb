using JetBrains.Annotations;
using Raven.Server.Documents.SchemaValidation;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Documents;

internal sealed class DocumentHandlerProcessorForValidateDocument : AbstractDocumentHandlerProcessorForValidateDocument<DocumentHandler, DocumentsOperationContext>
{
    public DocumentHandlerProcessorForValidateDocument([NotNull] DocumentHandler requestHandler) : base(requestHandler)
    {
    }

    protected override SchemaValidatorCache SchemaValidatorCache => RequestHandler.Database.SchemaValidatorCache;
}
