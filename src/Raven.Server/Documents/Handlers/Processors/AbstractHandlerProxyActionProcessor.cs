﻿using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.ServerWide;
using Raven.Server.Web;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors;

internal abstract class AbstractHandlerProxyActionProcessor<TRequestHandler, TOperationContext> : AbstractHandlerProxyProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractHandlerProxyActionProcessor([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
        : base(requestHandler, contextPool)
    {
    }

    protected abstract ValueTask ExecuteForCurrentNodeAsync();

    protected abstract Task ExecuteForRemoteNodeAsync(ProxyCommand command, OperationCancelToken token);

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

            using (var token = RequestHandler.CreateOperationToken())
                await ExecuteForRemoteNodeAsync(proxyCommand, token);
        }

        RequestHandler.NoContentStatus();
    }
}
