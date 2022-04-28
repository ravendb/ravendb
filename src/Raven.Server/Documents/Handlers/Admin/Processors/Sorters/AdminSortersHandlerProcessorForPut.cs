using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Sorters;

internal class AdminSortersHandlerProcessorForPut : AbstractAdminSortersHandlerProcessorForPut<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AdminSortersHandlerProcessorForPut([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }
}
