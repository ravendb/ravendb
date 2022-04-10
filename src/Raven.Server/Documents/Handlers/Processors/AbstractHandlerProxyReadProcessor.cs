using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Web;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors;

internal abstract class AbstractHandlerProxyReadProcessor<TResult, TRequestHandler, TOperationContext> : AbstractHandlerProxyProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractHandlerProxyReadProcessor([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
        : base(requestHandler, contextPool)
    {
    }

    protected abstract ValueTask HandleCurrentNodeAsync();

    protected abstract Task HandleRemoteNodeAsync(ProxyCommand<TResult> command);

    protected virtual RavenCommand<TResult> CreateCommandForNode(string nodeTag) => throw new NotSupportedException($"Processor '{GetType().Name}' does not support creating commands.");

    public override async ValueTask ExecuteAsync()
    {
        if (IsCurrentNode(out var nodeTag))
        {
            await HandleCurrentNodeAsync();
        }
        else
        {
            var command = CreateCommandForNode(nodeTag);
            var proxyCommand = new ProxyCommand<TResult>(command, RequestHandler.HttpContext.Response);
            await HandleRemoteNodeAsync(proxyCommand);
        }
    }
}
