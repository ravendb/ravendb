using JetBrains.Annotations;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Indexes;

internal class IndexHandlerProcessorForSource : AbstractIndexHandlerProcessorForSource<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexHandlerProcessorForSource([NotNull] DatabaseRequestHandler requestHandler) 
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override IndexInformationHolder GetIndex(string name) => RequestHandler.Database.IndexStore.GetIndex(name)?.ToIndexInformationHolder();
}
