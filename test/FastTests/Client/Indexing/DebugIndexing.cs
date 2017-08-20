using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Util;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.MoreLikeThis;
using Raven.Server.ServerWide;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Client.Indexing
{
    public class DebugIndexing : RavenTestBase
    {
        private class Person
        {
            public string Name { get; set; }
        }

        [Fact]
        public async Task QueriesRunning()
        {
            using (var store = GetDocumentStore())
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
                        Start = q.Start,
                        CutoffEtag = q.CutoffEtag,
                        DisableCaching = q.DisableCaching,
                        ExplainScores = q.ExplainScores,
                        HighlightedFields = q.HighlightedFields,
                        HighlighterKeyName = q.HighlighterKeyName,
                        HighlighterPostTags = q.HighlighterPostTags,
                        HighlighterPreTags = q.HighlighterPreTags,
                        PageSize = q.PageSize,
                        ShowTimings = q.ShowTimings,
                        SkipDuplicateChecking = q.SkipDuplicateChecking,
                        WaitForNonStaleResults = q.WaitForNonStaleResults,
                        WaitForNonStaleResultsAsOfNow = q.WaitForNonStaleResultsAsOfNow,
                        WaitForNonStaleResultsTimeout = q.WaitForNonStaleResultsTimeout
                    };

                    var query2 = new MoreLikeThisQueryServerSide
                    {
                        DocumentId = "docs/1"
                    };

                    var query3 = new FacetQuery
                    {
                        FacetSetupDoc = "setup/1"
                    };

                    var database = await Server
                        .ServerStore
                        .DatabasesLandlord
                        .TryGetOrCreateResourceStore(new StringSegment(store.Database));

                    var index = database.IndexStore.GetIndexes().First();

                    var now = SystemTime.UtcNow;
                    index.CurrentlyRunningQueries.TryAdd(new ExecutingQueryInfo(now, query1, 10, OperationCancelToken.None));
                    index.CurrentlyRunningQueries.TryAdd(new ExecutingQueryInfo(now, query2, 11, OperationCancelToken.None));
                    index.CurrentlyRunningQueries.TryAdd(new ExecutingQueryInfo(now, query3, 12, OperationCancelToken.None));

                    var conventions = new DocumentConventions();

                    using (var commands = store.Commands())
                    {
                        var json = commands.RawGetJson<BlittableJsonReaderObject>("/debug/queries/running");

                        Assert.True(json.TryGet(index.Name, out BlittableJsonReaderArray array));

                        Assert.Equal(3, array.Length);

                        foreach (BlittableJsonReaderObject info in array)
                        {
                            int queryId;
                            Assert.True(info.TryGet(nameof(ExecutingQueryInfo.QueryId), out queryId));

                            string duration;
                            Assert.True(info.TryGet(nameof(ExecutingQueryInfo.Duration), out duration));
                            Assert.NotNull(duration);

                            string startTimeAsString;
                            Assert.True(info.TryGet(nameof(ExecutingQueryInfo.StartTime), out startTimeAsString));
                            Assert.Equal(now, DateTime.Parse(startTimeAsString).ToUniversalTime());

                            object token;
                            Assert.False(info.TryGetMember(nameof(ExecutingQueryInfo.Token), out token));
                            Assert.Null(token);

                            if (queryId == 10)
                            {
                                BlittableJsonReaderObject queryInfo;
                                Assert.True(info.TryGet(nameof(ExecutingQueryInfo.QueryInfo), out queryInfo));

                                var query = (IndexQuery)conventions.DeserializeEntityFromBlittable(typeof(IndexQuery), queryInfo);

                                Assert.True(q.Equals(query));
                                continue;
                            }

                            if (queryId == 11)
                            {
                                BlittableJsonReaderObject queryInfo;
                                Assert.True(info.TryGet(nameof(ExecutingQueryInfo.QueryInfo), out queryInfo));

                                var query = (MoreLikeThisQuery)conventions.DeserializeEntityFromBlittable(typeof(MoreLikeThisQuery), queryInfo);

                                Assert.Equal(query2.DocumentId, query.DocumentId);
                                continue;
                            }

                            if (queryId == 12)
                            {
                                BlittableJsonReaderObject queryInfo;
                                Assert.True(info.TryGet(nameof(ExecutingQueryInfo.QueryInfo), out queryInfo));

                                var query = (FacetQuery)conventions.DeserializeEntityFromBlittable(typeof(FacetQuery), queryInfo);

                                Assert.Equal(query3.FacetSetupDoc, query.FacetSetupDoc);
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
