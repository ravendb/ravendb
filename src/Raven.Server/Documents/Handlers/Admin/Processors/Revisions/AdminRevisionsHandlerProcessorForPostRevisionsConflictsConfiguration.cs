using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Revisions
{
    internal sealed class AdminRevisionsHandlerProcessorForPostRevisionsConflictsConfiguration : AbstractAdminRevisionsHandlerProcessorForPostRevisionsConflictsConfiguration<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public AdminRevisionsHandlerProcessorForPostRevisionsConflictsConfiguration([NotNull] DatabaseRequestHandler requestHandler) 
            : base(requestHandler)
        {
        }
    }
}
