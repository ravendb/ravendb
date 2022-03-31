using JetBrains.Annotations;

namespace Raven.Server.Documents.Handlers.Processors.Sorters;

internal class SortersHandlerProcessorForGet : AbstractSortersHandlerProcessorForGet<DatabaseRequestHandler>
{
    public SortersHandlerProcessorForGet([NotNull] DatabaseRequestHandler requestHandler) 
        : base(requestHandler)
    {
    }

    protected override string GetDatabaseName() => RequestHandler.Database.Name;
}
