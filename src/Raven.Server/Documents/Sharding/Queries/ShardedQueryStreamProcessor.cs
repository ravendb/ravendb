using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Commands.Streaming;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Documents.Sharding.Comparers;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Handlers.Processors.Streaming;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Queries
{
    internal sealed class ShardedQueryStreamProcessor : AbstractShardedQueryProcessor<PostQueryStreamCommand, StreamResult, ShardedStreamQueryResult>
    {
        private readonly string _debug;
        private readonly bool _ignoreLimit;
        private List<OrderByField> _groupByFields;

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

            if (IsAutoMapReduceQuery || IndexType.IsMapReduce())
            {
                _groupByFields = new List<OrderByField>();
                var groupByFieldNames = GetGroupByFields();

                if (Query.Metadata.OrderBy != null)
                {
                    foreach (var orderByField in Query.Metadata.OrderBy)
                    {
                        if (groupByFieldNames.Contains(orderByField.Name.Value) == false)
                        {
                            throw new NotSupportedInShardingException($"Ordering by field '{orderByField.Name.Value}' which is not part of the 'group by' clause is not supported in sharded streaming queries.");
                        }

                        _groupByFields.Add(orderByField);
                    }
                }

                foreach (var groupByField in groupByFieldNames)
                {
                    if (Query.Metadata.OrderBy != null && Query.Metadata.OrderByFieldNames.Contains(groupByField))
                        continue;

                    _groupByFields.Add(new OrderByField(new QueryFieldName(groupByField, isQuoted: false), OrderByFieldType.Implicit, ascending: true));
                }
            }

            if (Query.Metadata.HasIncludeOrLoad)
                throw new NotSupportedInShardingException("Includes and Loads are not supported in sharded streaming queries");
        }

        public override Task<ShardedStreamQueryResult> ExecuteShardedOperations(QueryTimingsScope scope)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Stav, DevelopmentHelper.Severity.Normal, "Handle continuation token in streaming");
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Stav, DevelopmentHelper.Severity.Normal, "Missing scope usage");

            Func<ShardedDatabaseContext, string, IndexQueryServerSide, IComparer<BlittableJsonReaderObject>> documentsComparer = string.IsNullOrEmpty(_debug) 
                ? ComparerCreator
                : (_, _, _) => ConstantComparer.Instance;

            var commands = GetOperationCommands(null);
            var op = new ShardedStreamQueryOperation(RequestHandler.HttpContext, () =>
            {
                IDisposable returnToContextPool = RequestHandler.ContextPool.AllocateOperationContext(out JsonOperationContext ctx);
                return (ctx, returnToContextPool);
            }, documentsComparer, commands, Query, RequestHandler.DatabaseContext, _groupByFields, IsProjectionFromMapReduceIndex, Context, Token);

            var shards = GetShardNumbers(commands);

            return RequestHandler.ShardExecutor.ExecuteParallelForShardsAsync(shards, op, Token);
        }

        protected override PostQueryStreamCommand CreateCommand(int shardNumber, BlittableJsonReaderObject query, QueryTimingsScope scope)
        {
            return new PostQueryStreamCommand(RequestHandler.ShardExecutor.Conventions, query, _debug, _ignoreLimit);
        }
    }
}
