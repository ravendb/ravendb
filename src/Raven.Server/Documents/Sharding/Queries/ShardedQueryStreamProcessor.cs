using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Commands.Streaming;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Documents.Sharding.Comparers;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Handlers.Processors.Streaming;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Queries
{
    internal class ShardedQueryStreamProcessor : AbstractShardedQueryProcessor<PostQueryStreamCommand, StreamResult, (IEnumerator<BlittableJsonReaderObject>, StreamQueryStatistics)>
    {
        private readonly string _debug;
        private readonly bool _ignoreLimit;

        public ShardedQueryStreamProcessor(
            TransactionOperationContext context,
            ShardedDatabaseRequestHandler requestHandler,
            IndexQueryServerSide query,
            string debug,
            bool ignoreLimit,
            CancellationToken token)
            : base(context, requestHandler, query, metadataOnly: false, indexEntriesOnly: false, ignoreLimit: false, existingResultEtag: null, token)
        {
            _debug = debug;
            _ignoreLimit = ignoreLimit;
        }

        protected override void AssertQueryExecution()
        {
            base.AssertQueryExecution();

            if (IsAutoMapReduceQuery || IsMapReduceIndex)
                throw new NotSupportedInShardingException("MapReduce is not supported in sharded streaming queries");

            if (Query.Metadata.HasIncludeOrLoad)
                throw new NotSupportedInShardingException("Includes and Loads are not supported in sharded streaming queries");
        }

        public override Task<(IEnumerator<BlittableJsonReaderObject>, StreamQueryStatistics)> ExecuteShardedOperations(QueryTimingsScope scope)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Stav, DevelopmentHelper.Severity.Normal, "Handle continuation token in streaming");

            var documentsComparer = GetComparer(Query);

            var commands = GetOperationCommands(null);

            var op = new ShardedStreamQueryOperation(RequestHandler.HttpContext, () =>
            {
                IDisposable returnToContextPool = RequestHandler.ContextPool.AllocateOperationContext(out JsonOperationContext ctx);
                return (ctx, returnToContextPool);
            }, documentsComparer, commands, skip: Query.Offset ?? 0, take: Query.Limit ?? int.MaxValue, Token);

            var shards = GetShardNumbers(commands);

            return RequestHandler.ShardExecutor.ExecuteParallelForShardsAsync(shards, op, Token);
        }

        protected override PostQueryStreamCommand CreateCommand(int shardNumber, BlittableJsonReaderObject query, QueryTimingsScope scope)
        {
            return new PostQueryStreamCommand(RequestHandler.ShardExecutor.Conventions, query, _debug, _ignoreLimit);
        }
    }
}
