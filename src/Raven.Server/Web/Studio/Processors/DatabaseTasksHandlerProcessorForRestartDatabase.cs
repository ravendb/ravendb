using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Web.Studio.Processors;

internal sealed class DatabaseTasksHandlerProcessorForRestartDatabase : AbstractDatabaseTasksHandlerProcessorForRestartDatabase<DatabaseRequestHandler, DocumentsOperationContext>
{
    public DatabaseTasksHandlerProcessorForRestartDatabase([NotNull] DatabaseRequestHandler requestHandler, string urlSuffix) : base(requestHandler, urlSuffix)
    {
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
}
