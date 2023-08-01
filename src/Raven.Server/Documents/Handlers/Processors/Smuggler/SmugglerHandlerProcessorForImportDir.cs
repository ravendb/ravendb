using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents.Handlers;

namespace Raven.Server.Documents.Handlers.Processors.Smuggler;

internal sealed class SmugglerHandlerProcessorForImportDir : AbstractSmugglerHandlerProcessorForImportDir<SmugglerHandler, DocumentsOperationContext>
{
    public SmugglerHandlerProcessorForImportDir([NotNull] SmugglerHandler requestHandler) : base(requestHandler)
    {
    }

    protected override ImportDelegate DoImport => RequestHandler.DoImportInternalAsync;

    protected override long GetOperationId() => RequestHandler.Database.Operations.GetNextOperationId();
}
