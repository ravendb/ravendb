using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Raven.Server.Config;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace FastTests.Client.Indexing
{
    public class DebugIndexing : RavenTestBase
    {
        public DebugIndexing(ITestOutputHelper output) : base(output)
        {
        }

        private class Person
        {
            public string Name { get; set; }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task QueriesRunning(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                IndexQuery q;
                using (var session = store.OpenSession())
                {
                    var people = session.Query<Person>()
                        .Where(x => x.Name == "John")
                        .ToList(); // create index

                    q = session.Advanced.DocumentQuery<Person>()
                        .WhereEquals(x => x.Name, "John")
                        .Take(20)
                        .GetIndexQuery();
                }
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var query1 = new IndexQueryServerSide(q.Query, context.ReadObject(new DynamicJsonValue
                    {
                        ["p0"] = q.QueryParameters["p0"]
                    }, "query/parameters"))
                    {
#pragma warning disable 618
                        Start = q.Start,
                        PageSize = q.PageSize,
#pragma warning restore 618
                        SkipDuplicateChecking = q.SkipDuplicateChecking,
                        WaitForNonStaleResults = q.WaitForNonStaleResults,
                        WaitForNonStaleResultsTimeout = q.WaitForNonStaleResultsTimeout
                    };

                    var database = await Server
                        .ServerStore
                        .DatabasesLandlord
                        .TryGetOrCreateResourceStore(new StringSegment(store.Database));

                    var index = database.IndexStore.GetIndexes().First();

                    var marker = database.QueryRunner.MarkQueryAsRunning(index.Name, query1, OperationCancelToken.None);
                    var now = marker.StartTime;
                    var queryId = marker.QueryId;

                    using (var commands = store.Commands())
                    {
                        var json = commands.RawGetJson<BlittableJsonReaderObject>("/debug/queries/running");

                        Assert.True(json.TryGet(index.Name, out BlittableJsonReaderArray array));

                        Assert.Equal(1, array.Length);

                        foreach (BlittableJsonReaderObject info in array)
                        {
                            Assert.True(info.TryGet(nameof(ExecutingQueryInfo.QueryId), out long actualQueryId));

                            Assert.True(info.TryGet(nameof(ExecutingQueryInfo.Duration), out string duration));
                            Assert.NotNull(duration);

                            Assert.True(info.TryGet(nameof(ExecutingQueryInfo.IndexName), out string indexName));
                            Assert.Equal(index.Name, indexName);

                            Assert.True(info.TryGet(nameof(ExecutingQueryInfo.StartTime), out string startTimeAsString));
                            Assert.Equal(now, DateTime.Parse(startTimeAsString).ToUniversalTime());

                            Assert.False(info.TryGetMember(nameof(ExecutingQueryInfo.Token), out object token));
                            Assert.Null(token);

                            if (actualQueryId == queryId)
                            {
                                BlittableJsonReaderObject queryInfo;
                                Assert.True(info.TryGet(nameof(ExecutingQueryInfo.QueryInfo), out queryInfo));

                                var query = (IndexQuery)store.Conventions.Serialization.DeserializeEntityFromBlittable(typeof(IndexQuery), queryInfo);

                                Assert.True(q.Equals(query));
                                continue;
                            }

                            throw new NotSupportedException("Should not happen.");
                        }
                    }
                }
            }
        }
    }
}
