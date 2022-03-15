using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors;

internal abstract class AbstractHandlerReadProcessor<TResult, TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractHandlerReadProcessor([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
        : base(requestHandler, contextPool)
    {
    }

    protected abstract bool SupportsCurrentNode { get; }

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

        await WriteResultAsync(result);
    }

    protected int GetShardNumber()
    {
        return RequestHandler.GetIntValueQueryString("shardNumber", required: true).Value;
    }

    private bool IsCurrentNode(out string nodeTag)
    {
        nodeTag = GetNodeTag(required: SupportsCurrentNode == false);

        if (SupportsCurrentNode == false)
            return false;

        if (nodeTag == null)
            return true;

        return string.Equals(nodeTag, RequestHandler.ServerStore.NodeTag, StringComparison.OrdinalIgnoreCase);
    }

    private string GetNodeTag(bool required)
    {
        return RequestHandler.GetStringQueryString("nodeTag", required);
    }
}
