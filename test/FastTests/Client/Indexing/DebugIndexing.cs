using System;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Bundles.MoreLikeThis;
using Raven.Client.Connection;
using Raven.Client.Data;
using Raven.Client.Data.Queries;
using Raven.Client.Indexing;
using Raven.Json.Linq;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.MoreLikeThis;
using Raven.Server.ServerWide;

using Sparrow;

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
                        .GetIndexQuery(isAsync: false);
                }

                var query1 = new IndexQueryServerSide
                {
                    Transformer = q.Transformer,
                    Start = q.Start,
                    AllowMultipleIndexEntriesForSameDocumentToResultTransformer = q.AllowMultipleIndexEntriesForSameDocumentToResultTransformer,
                    CutoffEtag = q.CutoffEtag,
                    DebugOptionGetIndexEntries = q.DebugOptionGetIndexEntries,
                    DefaultField = q.DefaultField,
                    DefaultOperator = q.DefaultOperator,
                    DisableCaching = q.DisableCaching,
                    DynamicMapReduceFields = q.DynamicMapReduceFields,
                    ExplainScores = q.ExplainScores,
                    FieldsToFetch = q.FieldsToFetch,
                    HighlightedFields = q.HighlightedFields,
                    HighlighterKeyName = q.HighlighterKeyName,
                    HighlighterPostTags = q.HighlighterPostTags,
                    HighlighterPreTags = q.HighlighterPreTags,
                    Includes = q.Includes,
                    IsDistinct = q.IsDistinct,
                    PageSize = q.PageSize,
                    Query = q.Query,
                    ShowTimings = q.ShowTimings,
                    SkipDuplicateChecking = q.SkipDuplicateChecking,
                    SortedFields = q.SortedFields,
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
                    .TryGetOrCreateResourceStore(new StringSegment(store.DefaultDatabase, 0));

                var index = database.IndexStore.GetIndex(1);

                var now = SystemTime.UtcNow;
                index.CurrentlyRunningQueries.TryAdd(new ExecutingQueryInfo(now, query1, 10, OperationCancelToken.None));
                index.CurrentlyRunningQueries.TryAdd(new ExecutingQueryInfo(now, query2, 11, OperationCancelToken.None));
                index.CurrentlyRunningQueries.TryAdd(new ExecutingQueryInfo(now, query3, 12, OperationCancelToken.None));

                string jsonString;
                using (var client = new HttpClient())
                {
                    jsonString = await client.GetStringAsync($"{store.Url.ForDatabase(store.DefaultDatabase)}/debug/queries/running");
                }

                var json = RavenJObject.Parse(jsonString);
                var array = json.Value<RavenJArray>(index.Name);

                Assert.Equal(3, array.Length);

                foreach (var info in array)
                {
                    var queryId = info.Value<int>(nameof(ExecutingQueryInfo.QueryId));

                    Assert.NotNull(array[0].Value<string>(nameof(ExecutingQueryInfo.Duration)));
                    Assert.Equal(now, info.Value<DateTime>(nameof(ExecutingQueryInfo.StartTime)));
                    Assert.Null(info.Value<OperationCancelToken>(nameof(ExecutingQueryInfo.Token)));

                    if (queryId == 10)
                    {
                        var query = info
                            .Value<RavenJObject>(nameof(ExecutingQueryInfo.QueryInfo))
                            .JsonDeserialization<IndexQuery>();

                        Assert.True(q.Equals(query));
                        continue;
                    }

                    if (queryId == 11)
                    {
                        var query = info
                            .Value<RavenJObject>(nameof(ExecutingQueryInfo.QueryInfo))
                            .JsonDeserialization<MoreLikeThisQuery>();

                        Assert.Equal(query2.DocumentId, query.DocumentId);
                        continue;
                    }

                    if (queryId == 12)
                    {
                        var query = info
                            .Value<RavenJObject>(nameof(ExecutingQueryInfo.QueryInfo))
                            .JsonDeserialization<FacetQuery>();

                        Assert.Equal(query3.FacetSetupDoc, query.FacetSetupDoc);
                        continue;
                    }

                    throw new NotSupportedException("Should not happen.");
                }
            }
        }
    }
}