using System;
using System.Linq;
using System.Net.Http;
using System.Text;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
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

        [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.Sharding)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void GetShardedCollectionDocs(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Address() { City = "city1" }, "add/1");
                    session.Store(new User() { Name = "user1" }, "users/1");
                    session.Store(new Company() { Name = "com1" }, "com/1");
                    session.Store(new User() { Name = "user2" }, "users/2");
                    session.Store(new User() { Name = "user3" }, "users/3");
                    session.Store(new Company() { Name = "com2" }, "com/2");

                    session.SaveChanges();
                }

                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var collectionStats = store.Maintenance.Send(new GetCollectionOperation(context, collectionName: "Users", start: 0, pageSize: 2));
                    Assert.Equal(2, collectionStats.Results.Length);
                    var list = collectionStats.Results.Select(x => ((BlittableJsonReaderObject)x).GetMetadata().GetId()).ToList();
                    Assert.All(list, id => id.Contains("users"));

                    if (options.DatabaseMode == RavenDatabaseMode.Sharded)
                    {
                        collectionStats = store.Maintenance.Send(new GetCollectionOperation(context, "Users", collectionStats.ContinuationToken));
                    }
                    else
                    {
                        collectionStats = store.Maintenance.Send(new GetCollectionOperation(context, "Users", 2, 1));
                    }

                    Assert.Equal(1, collectionStats.Results.Length);
                    Assert.DoesNotContain(((BlittableJsonReaderObject)collectionStats.Results[0]).GetMetadata().GetId(), list); //assert we didn't get the same doc twice
                }
            }
        }

        private class GetCollectionOperation : IMaintenanceOperation<CollectionResult>
        {
            private readonly string _continuation;
            private readonly JsonOperationContext _context;
            private readonly string _collectionName;
            private readonly int? _start;
            private readonly int? _pageSize;

            public GetCollectionOperation(JsonOperationContext context, string collectionName, int start, int pageSize)
            {
                _context = context;
                _collectionName = collectionName;
                _start = start;
                _pageSize = pageSize;
            }

            public GetCollectionOperation(JsonOperationContext context, string collectionName, string continuation)
            {
                _context = context;
                _collectionName = collectionName;
                _continuation = continuation;
            }

            public RavenCommand<CollectionResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new GetCollectionCommand(_context, _collectionName, _start, _pageSize, _continuation);
            }

            private class GetCollectionCommand : RavenCommand<CollectionResult>
            {
                private readonly string _continuation;
                private readonly JsonOperationContext _context;
                private readonly string _collectionName;
                private readonly int? _start;
                private readonly int? _pageSize;

                public GetCollectionCommand(JsonOperationContext context, string collectionName, int? start, int? pageSize, string continuation)
                {
                    _context = context;
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

                    sb.Append($"&name={Uri.EscapeDataString(_collectionName)}");

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
                    
                    var arrayResult = JsonDeserializationClient.BlittableArrayResult(response);

                    Result = new CollectionResult()
                    {
                        Results = arrayResult.Results.Clone(_context),
                        ContinuationToken = arrayResult.ContinuationToken
                    };
                }
            }
        }
    }
}
