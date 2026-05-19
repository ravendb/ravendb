using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.ETL;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.ETL;

internal sealed class ShardedTaskErrorsHandlerProcessorForDeleteErrors : AbstractHandlerProxyReadProcessor<object, ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedTaskErrorsHandlerProcessorForDeleteErrors([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => false;

    protected override ValueTask HandleCurrentNodeAsync() => throw new NotSupportedException();

    protected override RavenCommand<object> CreateCommandForNode(string nodeTag)
    {
        var names = RequestHandler.GetStringValuesQueryString("name", required: false);

        if (names.Count > 0)
        {
            var taskCategory = RequestHandler.GetEnumQueryString<TaskCategory>("type");
            return new DeleteNamedTaskErrorsCommand(names, taskCategory, nodeTag);
        }

        return new DeleteAllTaskErrorsCommand(nodeTag);
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token)
    {
        var shardNumber = GetShardNumber();

        return RequestHandler.DatabaseContext.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber, token.Token);
    }
}
