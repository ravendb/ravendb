using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.ServerWide;
using Raven.Server.Web;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors;

internal abstract class AbstractHandlerProxyReadProcessor<TResult, TRequestHandler, TOperationContext> : AbstractHandlerProxyProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractHandlerProxyReadProcessor([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract ValueTask HandleCurrentNodeAsync();

    protected abstract Task HandleRemoteNodeAsync(ProxyCommand<TResult> command, OperationCancelToken token);

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
            command.SelectedNodeTag = nodeTag;

            var proxyCommand = new ProxyCommand<TResult>(command, RequestHandler.HttpContext.Response);

            using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
                await HandleRemoteNodeAsync(proxyCommand, token);
        }
    }
}

internal abstract class AbstractServerHandlerProxyReadProcessor<TResult> : AbstractServerHandlerProxyProcessor
{
    protected AbstractServerHandlerProxyReadProcessor([NotNull] RequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    protected abstract ValueTask HandleCurrentNodeAsync();

    protected abstract Task HandleRemoteNodeAsync(ProxyCommand<TResult> command, JsonOperationContext context, OperationCancelToken token);

    protected virtual ValueTask<RavenCommand<TResult>> CreateCommandForNodeAsync(string nodeTag, JsonOperationContext context) => throw new NotSupportedException($"Processor '{GetType().Name}' does not support creating commands.");

    public override async ValueTask ExecuteAsync()
    {
        if (IsCurrentNode(out var nodeTag))
        {
            await HandleCurrentNodeAsync();
        }
        else
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var command = await CreateCommandForNodeAsync(nodeTag, context);
                var proxyCommand = new ProxyCommand<TResult>(command, RequestHandler.HttpContext.Response);

                using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
                    await HandleRemoteNodeAsync(proxyCommand, context, token);
            }
        }
    }
}
