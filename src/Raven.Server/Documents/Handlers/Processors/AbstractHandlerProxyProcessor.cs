using System;
using JetBrains.Annotations;
using Raven.Client;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors;

internal abstract class AbstractHandlerProxyProcessor<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractHandlerProxyProcessor([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract bool SupportsCurrentNode { get; }

    protected int GetShardNumber() => RequestHandler.GetIntValueQueryString(Constants.QueryString.ShardNumber, required: true).Value;

    protected bool TryGetShardNumber(out int shardNumber)
    {
        shardNumber = -1;

        var value = RequestHandler.GetIntValueQueryString(Constants.QueryString.ShardNumber, required: false);
        if (value == null)
            return false;

        shardNumber = value.Value;
        return true;
    }

    protected bool IsCurrentNode(out string nodeTag)
    {
        nodeTag = GetNodeTag(required: SupportsCurrentNode == false);

        if (SupportsCurrentNode == false)
            return false;

        if (nodeTag == null)
            return true;

        return string.Equals(nodeTag, RequestHandler.ServerStore.NodeTag, StringComparison.OrdinalIgnoreCase);
    }

    protected string GetNodeTag(bool required)
    {
        return RequestHandler.GetStringQueryString(Constants.QueryString.NodeTag, required);
    }
}
