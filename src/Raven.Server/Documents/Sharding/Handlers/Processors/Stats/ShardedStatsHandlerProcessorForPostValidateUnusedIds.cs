using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Http;
using Raven.Server.Documents.Commands;
using Raven.Server.Documents.Handlers.Processors.Stats;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Stats;

internal class ShardedStatsHandlerProcessorForPostValidateUnusedIds : AbstractStatsHandlerProcessorForPostValidateUnusedIds<ShardedDatabaseRequestHandler,
    TransactionOperationContext>
{
    public ShardedStatsHandlerProcessorForPostValidateUnusedIds([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override async Task ValidateUnusedIdsOnAllNodesAsync(HashSet<string> unusedIds, string databaseName,
        CancellationToken token)
    {
        var op = new ShardedValidateUnusedIdsOperation(RequestHandler,
            new ValidateUnusedIdsCommand.Parameters()
            {
                DatabaseIds = unusedIds
            });
        await RequestHandler.ShardExecutor.ExecuteParallelForAllThrowAggregatedFailure(op, token);
    }

    internal readonly struct ShardedValidateUnusedIdsOperation : IShardedOperation
    {
        private readonly ShardedDatabaseRequestHandler _handler;
        private readonly ValidateUnusedIdsCommand.Parameters _parameters;

        public ShardedValidateUnusedIdsOperation(ShardedDatabaseRequestHandler handler, ValidateUnusedIdsCommand.Parameters parameters)
        {
            _handler = handler;
            _parameters = parameters;
        }

        public HttpRequest HttpRequest => _handler.HttpContext.Request;

        public RavenCommand<object> CreateCommandForShard(int shardNumber) => new ValidateUnusedIdsCommand(_parameters);
    }
}
