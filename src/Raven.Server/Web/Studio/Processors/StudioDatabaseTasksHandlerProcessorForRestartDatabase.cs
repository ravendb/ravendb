using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Web.Studio.Processors;

internal sealed class StudioDatabaseTasksHandlerProcessorForRestartDatabase : AbstractStudioDatabaseTasksHandlerProcessorForRestartDatabase<DatabaseRequestHandler, DocumentsOperationContext>
{
    public StudioDatabaseTasksHandlerProcessorForRestartDatabase([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
}
