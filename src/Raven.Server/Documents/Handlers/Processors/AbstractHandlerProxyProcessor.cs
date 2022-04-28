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

    protected int GetShardNumber()
    {
        return RequestHandler.GetIntValueQueryString(Constants.QueryString.ShardNumber, required: true).Value;
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
