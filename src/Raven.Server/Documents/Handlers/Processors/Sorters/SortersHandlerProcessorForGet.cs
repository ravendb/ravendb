using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Sorters;

internal class SortersHandlerProcessorForGet : AbstractSortersHandlerProcessorForGet<DocumentsOperationContext>
{
    public SortersHandlerProcessorForGet([NotNull] DatabaseRequestHandler requestHandler) 
        : base(requestHandler)
    {
    }
}
