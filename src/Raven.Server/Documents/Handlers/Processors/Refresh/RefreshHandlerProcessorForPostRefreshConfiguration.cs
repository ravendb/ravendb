using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Refresh;

internal sealed class RefreshHandlerProcessorForPostRefreshConfiguration : AbstractRefreshHandlerProcessorForPostRefreshConfiguration<DatabaseRequestHandler, DocumentsOperationContext>
{
    public RefreshHandlerProcessorForPostRefreshConfiguration([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }
}
