using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal class IndexHandlerProcessorForGetIndexHistory : AbstractIndexHandlerProcessorForGetIndexHistory<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexHandlerProcessorForGetIndexHistory([NotNull] DatabaseRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override string GetDatabaseName() => RequestHandler.Database.Name;
}
