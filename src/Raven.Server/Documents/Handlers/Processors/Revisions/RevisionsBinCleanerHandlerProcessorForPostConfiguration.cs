using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Revisions;

internal sealed class RevisionsBinCleanerHandlerProcessorForPostConfiguration : AbstractRevisionsBinCleanerHandlerProcessorForPostConfiguration<DatabaseRequestHandler, DocumentsOperationContext>
{
    public RevisionsBinCleanerHandlerProcessorForPostConfiguration([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }
}
