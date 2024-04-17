using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal sealed class IndexHandlerProcessorForReset : AbstractIndexHandlerProcessorForReset<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexHandlerProcessorForReset([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    private const string SideBySideQueryParameterName = "isSideBySide";

    protected override ValueTask HandleCurrentNodeAsync()
    {
        var name = GetName();

        var sideBySideQueryParam = RequestHandler.GetBoolValueQueryString(SideBySideQueryParameterName, false);

        var sideBySide = false;
        
        if (sideBySideQueryParam.HasValue)
            sideBySide = sideBySideQueryParam.Value;
        
        RequestHandler.Database.IndexStore.ResetIndex(name, sideBySide);

        return ValueTask.CompletedTask;
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
}
