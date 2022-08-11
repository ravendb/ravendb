using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Commands.Streaming;
using Raven.Server.Documents.Queries;
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

        public ShardedQueryStreamProcessor(TransactionOperationContext context, ShardedDatabaseRequestHandler requestHandler, IndexQueryServerSide query, string debug, bool ignoreLimit, CancellationToken token) : base(context, requestHandler, query, false, false, token)
        {
            _debug = debug;
            _ignoreLimit = ignoreLimit;
        }

        protected override void AssertQueryExecution()
        {
            base.AssertQueryExecution();

            if (_isAutoMapReduceQuery || _isMapReduceIndex)
                throw new NotSupportedInShardingException("MapReduce is not supported in sharded streaming queries");

            if (_query.Metadata.HasIncludeOrLoad)
                throw new NotSupportedInShardingException("Includes and Loads are not supported in sharded streaming queries");
        }

        public override Task<(IEnumerator<BlittableJsonReaderObject>, StreamQueryStatistics)> ExecuteShardedOperations()
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Stav, DevelopmentHelper.Severity.Normal, "Handle continuation token in streaming");

            IComparer<BlittableJsonReaderObject> comparer = _query.Metadata.OrderBy?.Length > 0
                ? new ShardedDocumentsComparer(_query.Metadata, isMapReduce: false)
                : new ShardedStreamingHandlerProcessorForGetStreamQuery.DocumentBlittableLastModifiedComparer();

            var op = new ShardedStreamQueryOperation(_requestHandler.HttpContext, () =>
            {
                IDisposable returnToContextPool = _requestHandler.ContextPool.AllocateOperationContext(out JsonOperationContext ctx);
                return (ctx, returnToContextPool);
            }, comparer, _commands, skip: _query.Offset ?? 0, take: _query.Limit ?? int.MaxValue, _token);

            return _requestHandler.ShardExecutor.ExecuteParallelForShardsAsync(_commands.Keys.ToArray(), op, _token);
        }

        protected override PostQueryStreamCommand CreateCommand(BlittableJsonReaderObject query)
        {
            return new PostQueryStreamCommand(query, _debug, _ignoreLimit);
        }

        public override void Dispose()
        {
        }
    }
}
