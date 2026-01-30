using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.Documents.SchemaValidation;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Web.Studio.Processors;

internal sealed class StudioDocumentHandlerProcessorForValidateDocument : AbstractStudioDocumentHandlerProcessorForValidateDocument<DatabaseRequestHandler, DocumentsOperationContext>
{
    public StudioDocumentHandlerProcessorForValidateDocument([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override SchemaValidatorCache SchemaValidatorCache => RequestHandler.Database.SchemaValidatorCache;
}
