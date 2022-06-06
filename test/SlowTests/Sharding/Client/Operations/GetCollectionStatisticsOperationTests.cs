using System;
using System.Net.Http;
using System.Text;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Handlers.ContinuationTokens;
using Raven.Server.Documents.Sharding.Handlers.Processors.Collections;
using SlowTests.Core.Utils.Entities;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Client.Operations
{
    public class GetCollectionStatisticsOperationTests : RavenTestBase
    {
        public GetCollectionStatisticsOperationTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Sharding)]
        public void GetShardedCollectionStatsTests()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User() { Name = "user1" }, "users/1");
                    session.Store(new User() { Name = "user2" }, "users/2");
                    session.Store(new User() { Name = "user3" }, "users/3");
                    session.Store(new Company() { Name = "com1" }, "com/1");
                    session.Store(new Company() { Name = "com2" }, "com/2");
                    session.Store(new Address() { City = "city1" }, "add/1");

                    session.SaveChanges();
                }

                var collectionStats = store.Maintenance.Send(new GetCollectionStatisticsOperation());

                Assert.Equal(3, collectionStats.Collections.Count);
                Assert.Equal(6, collectionStats.CountOfDocuments);
                Assert.Equal(0, collectionStats.CountOfConflicts);

                var detailedCollectionStats = store.Maintenance.Send(new GetDetailedCollectionStatisticsOperation());

                Assert.Equal(3, detailedCollectionStats.Collections.Count);
                Assert.Equal(6, detailedCollectionStats.CountOfDocuments);
                Assert.Equal(0, detailedCollectionStats.CountOfConflicts);
                Assert.Equal(3, detailedCollectionStats.Collections["Users"].CountOfDocuments);
                Assert.Equal(2, detailedCollectionStats.Collections["Companies"].CountOfDocuments);
                Assert.Equal(1, detailedCollectionStats.Collections["Addresses"].CountOfDocuments);

            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Sharding, Skip = "Sharded EP missing support for getting docs by collection name, RavenDB-18645")]
        public void GetShardedCollectionDocs()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User() { Name = "user1" }, "users/1");
                    session.Store(new User() { Name = "user2" }, "users/2");
                    session.Store(new User() { Name = "user3" }, "users/3");
                    session.Store(new Company() { Name = "com1" }, "com/1");
                    session.Store(new Company() { Name = "com2" }, "com/2");
                    session.Store(new Address() { City = "city1" }, "add/1");

                    session.SaveChanges();
                }

                var collectionStats = store.Maintenance.Send(new GetCollectionOperation(collectionName: "Users", start: 0, pageSize: 2));
                Assert.Equal(2, collectionStats.Results.Count);

                collectionStats = store.Maintenance.Send(new GetCollectionOperation("Users", collectionStats.ContinuationToken));
                Assert.Equal(1, collectionStats.Results.Count);
            }
        }

        private class GetCollectionOperation : IMaintenanceOperation<CollectionResult>
        {
            private readonly string _continuation;
            private readonly string _collectionName;
            private readonly int? _start;
            private readonly int? _pageSize;

            public GetCollectionOperation(string collectionName, int start, int pageSize)
            {
                _collectionName = collectionName;
                _start = start;
                _pageSize = pageSize;
            }

            public GetCollectionOperation(string collectionName, string continuation)
            {
                _collectionName = collectionName;
                _continuation = continuation;
            }

            public RavenCommand<CollectionResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new GetCollectionCommand(_collectionName, _start, _pageSize, _continuation);
            }

            private class GetCollectionCommand : RavenCommand<CollectionResult>
            {
                private readonly string _continuation;
                private readonly string _collectionName;
                private readonly int? _start;
                private readonly int? _pageSize;

                public GetCollectionCommand(string collectionName, int? start, int? pageSize, string continuation)
                {
                    _collectionName = collectionName;
                    _start = start;
                    _pageSize = pageSize;
                    _continuation = continuation ?? string.Empty;
                }

                public override bool IsReadRequest => true;

                public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
                {
                    var sb = new StringBuilder();
                    sb.Append($"{node.Url}/databases/{node.Database}/collections/docs");
                    sb.Append($"?{ContinuationToken.ContinuationTokenQueryString}={Uri.EscapeDataString(_continuation)}");

                    sb.Append($"&collectionName={Uri.EscapeDataString(_collectionName)}");

                    if (_start.HasValue)
                        sb.Append($"&start={_start}");
                    if (_pageSize.HasValue)
                        sb.Append($"&pageSize={_pageSize}");

                    url = sb.ToString();
                    return new HttpRequestMessage
                    {
                        Method = HttpMethod.Get
                    };
                }

                public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
                {
                    if (response == null)
                        ThrowInvalidResponse();

                    Result = DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<CollectionResult>(response);
                }
            }
        }
    }
}
