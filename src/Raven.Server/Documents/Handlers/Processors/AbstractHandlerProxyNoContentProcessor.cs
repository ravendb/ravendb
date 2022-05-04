using System;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.ServerWide;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors;

internal abstract class AbstractHandlerProxyNoContentProcessor<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<object, TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    private readonly HttpStatusCode _statusCode;

    protected AbstractHandlerProxyNoContentProcessor([NotNull] TRequestHandler requestHandler, HttpStatusCode statusCode = HttpStatusCode.NoContent) : base(requestHandler)
    {
        _statusCode = statusCode;
    }

    public override async ValueTask ExecuteAsync()
    {
        await base.ExecuteAsync();

        RequestHandler.NoContentStatus(_statusCode);
    }
}

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
            var proxyCommand = new ProxyCommand<TResult>(command, RequestHandler.HttpContext.Response);

            using (var token = RequestHandler.CreateOperationToken())
                await HandleRemoteNodeAsync(proxyCommand, token);
        }
    }
}
