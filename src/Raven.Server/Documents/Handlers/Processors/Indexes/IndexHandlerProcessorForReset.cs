using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal sealed class IndexHandlerProcessorForReset : AbstractIndexHandlerProcessorForReset<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexHandlerProcessorForReset([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }
    
    private const string IndexResetModeQueryStringParamName = "mode";

    protected override bool SupportsCurrentNode => true;

    protected override ValueTask HandleCurrentNodeAsync()
    {
        var name = GetName();

        var indexResetModeQueryParam = RequestHandler.GetStringQueryString(IndexResetModeQueryStringParamName, false);

        var indexResetMode = RequestHandler.Database.Configuration.Indexing.ResetMode;

        if (indexResetModeQueryParam is not null)
            indexResetMode = Enum.Parse<IndexResetMode>(indexResetModeQueryParam);
        
        RequestHandler.Database.IndexStore.ResetIndex(name, indexResetMode);

        if (RavenLogManager.Instance.IsAuditEnabled)
        {
            RequestHandler.LogAuditFor(RequestHandler.DatabaseName, "RESET", $"Index '{name}'");
        }

        return ValueTask.CompletedTask;
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
}
