﻿using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Handlers.Processors.Indexes;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Indexes
{
    internal sealed class ShardedIndexHandlerProcessorForGetIndexesStatus : AbstractIndexHandlerProcessorForGetIndexesStatus<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedIndexHandlerProcessorForGetIndexesStatus([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
        protected override bool SupportsCurrentNode => false;

        protected override ValueTask HandleCurrentNodeAsync() => throw new NotSupportedException();

        protected override Task HandleRemoteNodeAsync(ProxyCommand<IndexingStatus> command, OperationCancelToken token)
        {
            var shardNumber = GetShardNumber();

            return RequestHandler.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber, token.Token);
        }
    }
}