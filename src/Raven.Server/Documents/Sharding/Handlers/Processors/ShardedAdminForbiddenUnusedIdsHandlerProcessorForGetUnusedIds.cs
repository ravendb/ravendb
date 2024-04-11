using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using NuGet.Packaging;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents.Commands.Revisions;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Handlers.Processors.Revisions;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.Handlers.Processors.Revisions;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors;

internal class ShardedValidateUnusedIdsHandlerProcessorForGet : AbstractValidateUnusedIdsHandlerProcessorForGet<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedValidateUnusedIdsHandlerProcessorForGet([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
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
        await RequestHandler.ShardExecutor.ExecuteParallelForAllThrowAggregateFailure(op, token);
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
