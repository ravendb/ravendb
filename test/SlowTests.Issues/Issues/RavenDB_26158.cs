using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.Operations.Lazy;
using Sparrow.Json;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_26158 : RavenTestBase
    {
        public RavenDB_26158(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void Can_add_query_tag_to_queries_url()
        {
            using var store = GetDocumentStore();

            StoreOrder(store);

            using var session = store.OpenSession();
            var inMemorySession = (InMemoryDocumentSessionOperations)session;

            IndexQuery linqIndexQuery = null;
            session.Query<Order>()
                .Customize(x =>
                {
                    x.WithTag("linq-tag");
                    x.BeforeQueryExecuted(iq => linqIndexQuery = iq);
                })
                .ToList();

            var documentQuery = (DocumentQuery<Order>)session.Advanced.DocumentQuery<Order>()
                .WithTag("document-query-tag");

            var rawQuery = (DocumentQuery<Order>)session.Advanced.RawQuery<Order>("from Orders")
                .WithTag("raw-query-tag");

            Assert.NotNull(linqIndexQuery);
            Assert.Equal("linq-tag", linqIndexQuery.Tag);

            var documentIndexQuery = documentQuery.GetIndexQuery();
            var rawIndexQuery = rawQuery.GetIndexQuery();

            Assert.Equal("document-query-tag", documentIndexQuery.Tag);
            Assert.Equal("raw-query-tag", rawIndexQuery.Tag);

            var linqUrl = CreateQueryRequestUrl(store, inMemorySession, linqIndexQuery);
            var documentQueryUrl = CreateQueryRequestUrl(store, inMemorySession, documentIndexQuery);
            var rawQueryUrl = CreateQueryRequestUrl(store, inMemorySession, rawIndexQuery);

            Assert.Contains("tag=linq-tag", linqUrl, StringComparison.Ordinal);
            Assert.Contains("tag=document-query-tag", documentQueryUrl, StringComparison.Ordinal);
            Assert.Contains("tag=raw-query-tag", rawQueryUrl, StringComparison.Ordinal);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void Can_url_encode_query_tag()
        {
            using var store = GetDocumentStore();

            StoreOrder(store);

            using var session = store.OpenSession();
            var inMemorySession = (InMemoryDocumentSessionOperations)session;

            var documentQuery = (DocumentQuery<Order>)session.Advanced.DocumentQuery<Order>()
                .WithTag("tag with space");

            var indexQuery = documentQuery.GetIndexQuery();

            Assert.Equal("tag with space", indexQuery.Tag);

            var queryUrl = CreateQueryRequestUrl(store, inMemorySession, indexQuery);

            Assert.Contains("tag=tag%20with%20space", queryUrl, StringComparison.Ordinal);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void Can_add_query_tag_to_lazy_queries_and_streaming_requests()
        {
            using var store = GetDocumentStore();

            StoreOrder(store);

            using var session = store.OpenSession();
            var inMemorySession = (InMemoryDocumentSessionOperations)session;
            var requestExecutor = store.GetRequestExecutor();

            using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var lazyDocumentQuery = (DocumentQuery<Order>)session.Advanced.DocumentQuery<Order>()
                    .WithTag("lazy-query-tag");

                var lazyQueryRequest = new LazyQueryOperation<Order>(inMemorySession, lazyDocumentQuery.InitializeQueryOperation(), afterQueryExecuted: null)
                    .CreateRequest(context);

                Assert.Contains("tag=lazy-query-tag", lazyQueryRequest.Query, StringComparison.Ordinal);

                var lazySuggestionRequest = new LazySuggestionQueryOperation(
                        inMemorySession,
                        new IndexQuery { Query = "from Orders", Tag = "lazy-suggestion-tag" },
                        invokeAfterQueryExecuted: null,
                        processResults: _ => null)
                    .CreateRequest(context);

                Assert.Contains("tag=lazy-suggestion-tag", lazySuggestionRequest.Query, StringComparison.Ordinal);

                var lazyAggregationRequest = new LazyAggregationQueryOperation(
                        inMemorySession,
                        new IndexQuery { Query = "from Orders", Tag = "lazy-aggregation-tag" },
                        invokeAfterQueryExecuted: null,
                        processResults: _ => null)
                    .CreateRequest(context);

                Assert.Contains("tag=lazy-aggregation-tag", lazyAggregationRequest.Query, StringComparison.Ordinal);
            }

            var streamUrl = CreateStreamQueryRequestUrl(store, new IndexQuery { Query = "from Orders", Tag = "stream-tag" });

            Assert.Contains("tag=stream-tag", streamUrl, StringComparison.Ordinal);
        }

        private static string CreateQueryRequestUrl(IDocumentStore store, InMemoryDocumentSessionOperations session, IndexQuery indexQuery)
        {
            var requestExecutor = store.GetRequestExecutor();
            var command = new QueryCommand(session, indexQuery);

            using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var (_, node) = requestExecutor.ChooseNodeForRequest(command);
                using var request = command.CreateRequest(context, node, out var url);
                return url;
            }
        }

        private static string CreateStreamQueryRequestUrl(IDocumentStore store, IndexQuery indexQuery)
        {
            var requestExecutor = store.GetRequestExecutor();
            var command = new QueryStreamCommand(store.Conventions, indexQuery);

            using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var (_, node) = requestExecutor.ChooseNodeForRequest(command);
                using var request = command.CreateRequest(context, node, out var url);
                return url;
            }
        }

        private static void StoreOrder(IDocumentStore store)
        {
            using var session = store.OpenSession();
            session.Store(new Order(), "orders/1");
            session.SaveChanges();
        }
    }
}
