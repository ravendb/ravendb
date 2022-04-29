using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Web;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors;

internal abstract class AbstractHandlerProxyActionProcessor<TRequestHandler, TOperationContext> : AbstractHandlerProxyProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext 
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractHandlerProxyActionProcessor([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract ValueTask ExecuteForCurrentNodeAsync();

    protected abstract Task ExecuteForRemoteNodeAsync(ProxyCommand command);

    protected virtual RavenCommand CreateCommandForNode(string nodeTag) => throw new NotSupportedException($"Processor '{GetType().Name}' does not support creating commands.");

    public override async ValueTask ExecuteAsync()
    {
        if (IsCurrentNode(out var nodeTag))
        {
            await ExecuteForCurrentNodeAsync();
        }
        else
        {
            var command = CreateCommandForNode(nodeTag);
            var proxyCommand = new ProxyCommand(command, RequestHandler.HttpContext.Response);
            await ExecuteForRemoteNodeAsync(proxyCommand);
        }

        RequestHandler.NoContentStatus();
    }
}
