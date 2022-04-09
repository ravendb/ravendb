using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Web;
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

    protected abstract ValueTask<TResult> GetResultForCurrentNodeAsync();

    protected abstract Task<TResult> GetResultForRemoteNodeAsync(RavenCommand<TResult> command);

    protected abstract ValueTask WriteResultAsync(TResult result);

    protected virtual RavenCommand<TResult> CreateCommandForNode(string nodeTag) => throw new NotSupportedException($"Processor '{GetType().Name}' does not support creating commands.");

    public override async ValueTask ExecuteAsync()
    {
        TResult result;
        if (IsCurrentNode(out var nodeTag))
        {
            result = await GetResultForCurrentNodeAsync();
        }
        else
        {
            var command = CreateCommandForNode(nodeTag);
            result = await GetResultForRemoteNodeAsync(command);
        }

        if (result != null)
            await WriteResultAsync(result);
    }
}
