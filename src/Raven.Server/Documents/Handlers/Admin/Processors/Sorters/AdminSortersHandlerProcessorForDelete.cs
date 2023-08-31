using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Sorters;

internal sealed class AdminSortersHandlerProcessorForDelete : AbstractAdminSortersHandlerProcessorForDelete<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AdminSortersHandlerProcessorForDelete([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }
}
