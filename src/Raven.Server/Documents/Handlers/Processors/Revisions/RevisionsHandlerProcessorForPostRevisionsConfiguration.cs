using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Revisions;

internal class RevisionsHandlerProcessorForPostRevisionsConfiguration : AbstractRevisionsHandlerProcessorForPostRevisionsConfiguration<DatabaseRequestHandler, DocumentsOperationContext>
{
    public RevisionsHandlerProcessorForPostRevisionsConfiguration([NotNull] DatabaseRequestHandler requestHandler) 
        : base(requestHandler)
    {
    }
}
