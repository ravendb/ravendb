using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors.Indexes;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Documents.Sharding.Streaming;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Indexes
{
    internal class ShardedIndexHandlerProcessorForTerms : AbstractIndexHandlerProcessorForTerms<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedIndexHandlerProcessorForTerms([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async ValueTask<TermsQueryResultServerSide> GetTermsAsync(string indexName, string field, string fromValue, int pageSize)
        {
            var op = new ShardedGetTermsOperation(RequestHandler, indexName, field, fromValue, pageSize);
            var result = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op);

            return result;
        }


        private readonly struct ShardedGetTermsOperation : IShardedOperation<TermsQueryResultServerSide>
        {
            private readonly ShardedDatabaseRequestHandler _handler;
            private readonly string _indexName;
            private readonly string _field;
            private readonly string _fromValue;
            private readonly int _pageSize;

            public ShardedGetTermsOperation(ShardedDatabaseRequestHandler handler, string indexName, string field, string fromValue, int pageSize)
            {
                _handler = handler;
                _indexName = indexName;
                _field = field;
                _fromValue = fromValue;
                _pageSize = pageSize;
            }

            public HttpRequest HttpRequest => _handler.HttpContext.Request;

            public TermsQueryResultServerSide Combine(Memory<TermsQueryResultServerSide> results)
            {
                var pageSize = _pageSize;
                var terms = new HashSet<string>();
                foreach (var res in _handler.DatabaseContext.Streaming.CombinedResults(results, r => r.Terms, TermsComparer.Instance))
                {
                    if (terms.Add(res.Item))
                        pageSize--;

                    if (pageSize <= 0)
                        break;
                }

                return new TermsQueryResultServerSide { IndexName = results.Span[0].IndexName, ResultEtag = results.Span[0].ResultEtag, Terms = terms };
            }

            public RavenCommand<TermsQueryResultServerSide> CreateCommandForShard(int shard) => new GetIndexTermsCommand(indexName: _indexName, field: _field, _fromValue, _pageSize);
        }

        public class TermsComparer : Comparer<ShardStreamItem<string>>
        {
            public override int Compare(ShardStreamItem<string> x,
                ShardStreamItem<string> y)
            {
                return string.CompareOrdinal(x?.Item, y?.Item);
            }

            public static TermsComparer Instance = new();
        }
    }
}
