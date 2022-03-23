using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents.Commands;
using Raven.Server.Documents.Commands.Indexes;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Studio.Processors;

namespace Raven.Server.Web.Studio.Sharding.Processors;

internal class ShardedStudioIndexHandlerProcessorForGetIndexErrorsCount : AbstractStudioIndexHandlerProcessorForGetIndexErrorsCount<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedStudioIndexHandlerProcessorForGetIndexErrorsCount([NotNull] ShardedDatabaseRequestHandler requestHandler) 
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override bool SupportsCurrentNode => false;

    protected override ValueTask<GetIndexErrorsCountCommand.IndexErrorsCount[]> GetResultForCurrentNodeAsync() => throw new NotSupportedException();

    protected override Task<GetIndexErrorsCountCommand.IndexErrorsCount[]> GetResultForRemoteNodeAsync(RavenCommand<GetIndexErrorsCountCommand.IndexErrorsCount[]> command) => RequestHandler.ShardExecutor.ExecuteSingleShardAsync(command, GetShardNumber());
}
