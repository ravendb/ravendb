using JetBrains.Annotations;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Indexes;

internal sealed class AdminIndexHandlerProcessorForStaticPut : AbstractAdminIndexHandlerProcessorForStaticPut<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AdminIndexHandlerProcessorForStaticPut([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override AbstractIndexCreateController GetIndexCreateProcessor() => RequestHandler.Database.IndexStore.Create;
}
