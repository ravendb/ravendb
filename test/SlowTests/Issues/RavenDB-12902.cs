using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12902 : RavenTestBase
    {
        public RavenDB_12902(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task AfterAggregationQueryExecutedShouldBeExecutedOnlyOnce()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new UsersByName());

                using (var session = store.OpenSession())
                {
                    var counter = 0;

                    QueryStatistics stats;

                    var results = session
                        .Query<User, UsersByName>()
                        .Customize(x =>
                        {
                            x.AfterQueryExecuted(r =>
                            {
                                Interlocked.Increment(ref counter);
                            });
                        })
                        .Statistics(out stats)
                        .Where(x => x.Name == "Doe")
                        .AggregateBy(x => x.ByField(y => y.Name).SumOn(y => y.Count))

                        .Execute();

                    Assert.Equal(1, results.Count);
                    Assert.Equal(0, results["Name"].Values.Count);
                    Assert.NotNull(stats);
                    Assert.Equal(1, counter);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var counter = 0;

                    QueryStatistics stats;

                    var results = await session
                        .Query<User, UsersByName>()
                        .Customize(x => x.AfterQueryExecuted(r => { Interlocked.Increment(ref counter); }))
                        .Statistics(out stats)
                        .AggregateBy(x => x.ByField(y => y.Name).SumOn(y => y.Count))
                        .ExecuteAsync();

                    Assert.Equal(1, results.Count);
                    Assert.Equal(0, results["Name"].Values.Count);

                    Assert.NotNull(stats);
                    Assert.Equal(1, counter);
                }

                using (var session = store.OpenSession())
                {
                    var counter = 0;

                    QueryStatistics stats;

                    var query = session
                        .Advanced
                        .DocumentQuery<User, UsersByName>();

                    query.AfterQueryExecuted(r => { Interlocked.Increment(ref counter); });
                    query.Statistics(out stats);

                    var results = query.AggregateBy(x => x.ByField(y => y.Name).SumOn(y => y.Count))
                        .Execute();

                    Assert.Equal(1, results.Count);
                    Assert.Equal(0, results["Name"].Values.Count);
                    Assert.NotNull(stats);
                    Assert.Equal(1, counter);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var counter = 0;

                    QueryStatistics stats;
                    var query = session
                        .Advanced
                        .AsyncDocumentQuery<User, UsersByName>();

                    query.AfterQueryExecuted(r => { Interlocked.Increment(ref counter); });
                    query.Statistics(out stats);

                    var results = await query.AggregateBy(x => x.ByField(y => y.Name).SumOn(y => y.Count))
                        .ExecuteAsync();

                    Assert.Equal(1, results.Count);
                    Assert.Equal(0, results["Name"].Values.Count);
                    Assert.NotNull(stats);
                    Assert.Equal(1, counter);
                }
            }
        }

        [Fact]
        public async Task AfterSuggestionQueryExecutedShouldBeExecutedOnlyOnce()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new UsersByName());

                using (var session = store.OpenSession())
                {
                    var counter = 0;

                    QueryStatistics stats;

                    var results = session
                        .Query<User, UsersByName>()
                        .Customize(x => x.AfterQueryExecuted(r => { Interlocked.Increment(ref counter); }))
                        .Statistics(out stats)
                        .SuggestUsing(x => x.ByField(y => y.Name, "Orin"))
                        .Execute();

                    Assert.Equal(1, results.Count);
                    Assert.Equal(0, results["Name"].Suggestions.Count);
                    Assert.NotNull(stats);
                    Assert.Equal(1, counter);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var counter = 0;

                    QueryStatistics stats;

                    var results = await session
                        .Query<User, UsersByName>()
                        .Customize(x => x.AfterQueryExecuted(r => { Interlocked.Increment(ref counter); }))
                        .Statistics(out stats)
                        .SuggestUsing(x => x.ByField(y => y.Name, "Orin"))
                        .ExecuteAsync();

                    Assert.Equal(1, results.Count);
                    Assert.Equal(0, results["Name"].Suggestions.Count);
                    Assert.NotNull(stats);
                    Assert.Equal(1, counter);
                }

                using (var session = store.OpenSession())
                {
                    var counter = 0;

                    QueryStatistics stats;

                    var query = session
                        .Advanced
                        .DocumentQuery<User, UsersByName>();

                    query.AfterQueryExecuted(r => { Interlocked.Increment(ref counter); });
                    query.Statistics(out stats);

                    var results = query.SuggestUsing(x => x.ByField(y => y.Name, "Orin"))
                        .Execute();

                    Assert.Equal(1, results.Count);
                    Assert.Equal(0, results["Name"].Suggestions.Count);
                    Assert.NotNull(stats);
                    Assert.Equal(1, counter);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var counter = 0;

                    QueryStatistics stats;
                    var query = session
                        .Advanced
                        .AsyncDocumentQuery<User, UsersByName>();

                    query.AfterQueryExecuted(r => { Interlocked.Increment(ref counter); });
                    query.Statistics(out stats);

                    var results = await query.SuggestUsing(x => x.ByField(y => y.Name, "Orin"))
                        .ExecuteAsync();

                    Assert.Equal(1, results.Count);
                    Assert.Equal(0, results["Name"].Suggestions.Count);
                    Assert.NotNull(stats);
                    Assert.Equal(1, counter);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task AfterLazyQueryExecutedShouldBeExecutedOnlyOnce(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var counter = 0;

                    QueryStatistics stats;

                    var results = session
                        .Query<User>()
                        .Customize(x => x.AfterQueryExecuted(r => { Interlocked.Increment(ref counter); }))
                        .Statistics(out stats)
                        .Where(x => x.Name == "Doe")
                        .Lazily()
                        .Value
                        .ToList();

                    Assert.Equal(0, results.Count);
                    Assert.NotNull(stats);
                    Assert.Equal(1, counter);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var counter = 0;

                    QueryStatistics stats;

                    var results = (await session
                            .Query<User>()
                            .Customize(x => x.AfterQueryExecuted(r => { Interlocked.Increment(ref counter); }))
                            .Statistics(out stats)
                            .Where(x => x.Name == "Doe")
                            .LazilyAsync()
                            .Value)
                        .ToList();

                    Assert.Equal(0, results.Count);
                    Assert.NotNull(stats);
                    Assert.Equal(1, counter);
                }

                using (var session = store.OpenSession())
                {
                    var counter = 0;

                    QueryStatistics stats;

                    var query = session
                        .Advanced
                        .DocumentQuery<User>();

                    query.AfterQueryExecuted(r => { Interlocked.Increment(ref counter); });
                    query.Statistics(out stats);

                    var results = query.WhereEquals("Name", "Doe")
                        .Lazily()
                        .Value
                        .ToList();

                    Assert.Equal(0, results.Count);
                    Assert.NotNull(stats);
                    Assert.Equal(1, counter);
                    Assert.Equal(1, counter);
                }


                using (var session = store.OpenAsyncSession())
                {
                    var counter = 0;

                    QueryStatistics stats;
                    var query = session
                        .Advanced
                        .AsyncDocumentQuery<User>();

                    query.AfterQueryExecuted(r => { Interlocked.Increment(ref counter); });
                    query.Statistics(out stats);

                    var results = (await query.WhereEquals("Name", "Doe")
                        .LazilyAsync()
                        .Value).ToList();

                    Assert.Equal(0, results.Count);
                    Assert.NotNull(stats);
                    Assert.Equal(1, counter);
                }
            }
        }

        [Fact]
        public async Task AfterLazyAggregationQueryExecutedShouldBeExecutedOnlyOnce()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new UsersByName());

                using (var session = store.OpenSession())
                {
                    var counter = 0;

                    QueryStatistics stats;

                    var results = session
                        .Query<User, UsersByName>()
                        .Customize(x => x.AfterQueryExecuted(r => { Interlocked.Increment(ref counter); }))
                        .Statistics(out stats)
                        .AggregateBy(x => x.ByField(y => y.Name).SumOn(y => y.Count))
                        .ExecuteLazy()
                        .Value;

                    Assert.Equal(1, results.Count);
                    Assert.Equal(0, results["Name"].Values.Count);
                    Assert.NotNull(stats);
                    Assert.Equal(1, counter);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var counter = 0;

                    QueryStatistics stats;

                    var results = (await session
                            .Query<User, UsersByName>()
                            .Customize(x => x.AfterQueryExecuted(r => { Interlocked.Increment(ref counter); }))
                            .Statistics(out stats)
                            .AggregateBy(x => x.ByField(y => y.Name).SumOn(y => y.Count))
                            .ExecuteLazyAsync()
                            .Value)
                        .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal(0, results[0].Value.Values.Count);

                    Assert.NotNull(stats);
                    Assert.Equal(1, counter);
                }

                using (var session = store.OpenSession())
                {
                    var counter = 0;

                    QueryStatistics stats;

                    var query = session
                        .Advanced
                        .DocumentQuery<User, UsersByName>();

                    query.AfterQueryExecuted(r => { Interlocked.Increment(ref counter); });
                    query.Statistics(out stats);

                    var results = query.AggregateBy(x => x.ByField(y => y.Name).SumOn(y => y.Count))
                        .ExecuteLazy().Value;

                    Assert.Equal(1, results.Count);
                    Assert.Equal(0, results["Name"].Values.Count);
                    Assert.NotNull(stats);
                    Assert.Equal(1, counter);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var counter = 0;

                    QueryStatistics stats;
                    var query = session
                        .Advanced
                        .AsyncDocumentQuery<User, UsersByName>();

                    query.AfterQueryExecuted(r => { Interlocked.Increment(ref counter); });
                    query.Statistics(out stats);

                    var results = (await query.AggregateBy(x => x.ByField(y => y.Name).SumOn(y => y.Count))
                        .ExecuteLazyAsync().Value).ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal(0, results[0].Value.Values.Count);
                    Assert.NotNull(stats);
                    Assert.Equal(1, counter);
                }
            }
        }

        [Fact]
        public async Task AfterLazySuggestionQueryExecutedShouldBeExecutedOnlyOnce()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new UsersByName());

                using (var session = store.OpenSession())
                {
                    var counter = 0;

                    QueryStatistics stats;

                    var results = session
                        .Query<User, UsersByName>()
                        .Customize(x => x.AfterQueryExecuted(r => { Interlocked.Increment(ref counter); }))
                        .Statistics(out stats)
                        .SuggestUsing(x => x.ByField(y => y.Name, "Orin"))
                        .ExecuteLazy()
                        .Value;

                    Assert.Equal(1, results.Count);
                    Assert.Equal(0, results["Name"].Suggestions.Count);
                    Assert.NotNull(stats);
                    Assert.Equal(1, counter);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var counter = 0;

                    QueryStatistics stats;

                    var results = (await session
                            .Query<User, UsersByName>()
                            .Customize(x => x.AfterQueryExecuted(r => { Interlocked.Increment(ref counter); }))
                            .Statistics(out stats)
                            .SuggestUsing(x => x.ByField(y => y.Name, "Orin"))
                            .ExecuteLazyAsync()
                            .Value)
                        .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal(0, results[0].Value.Suggestions.Count);
                    Assert.NotNull(stats);
                    Assert.Equal(1, counter);
                }

                using (var session = store.OpenSession())
                {
                    var counter = 0;

                    QueryStatistics stats;

                    var query = session
                        .Advanced
                        .DocumentQuery<User, UsersByName>();

                    query.AfterQueryExecuted(r => { Interlocked.Increment(ref counter); });
                    query.Statistics(out stats);

                    var results = query.SuggestUsing(x => x.ByField(y => y.Name, "Orin"))
                        .ExecuteLazy()
                        .Value;

                    Assert.Equal(1, results.Count);
                    Assert.Equal(0, results["Name"].Suggestions.Count);
                    Assert.NotNull(stats);
                    Assert.Equal(1, counter);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var counter = 0;

                    QueryStatistics stats;
                    var query = session
                        .Advanced
                        .AsyncDocumentQuery<User, UsersByName>();

                    query.AfterQueryExecuted(r => { Interlocked.Increment(ref counter); });
                    query.Statistics(out stats);

                    var results = (await query.SuggestUsing(x => x.ByField(y => y.Name, "Orin"))
                            .ExecuteLazyAsync()
                            .Value)
                        .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal(0, results[0].Value.Suggestions.Count);
                    Assert.NotNull(stats);
                    Assert.Equal(1, counter);
                }
            }
        }

        [Fact]
        public async Task CanGetValidStatisticsInAggregationQuery()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new UsersByName());

                using (var session = store.OpenSession())
                {
                    session
                        .Query<User, UsersByName>()
                        .Statistics(out QueryStatistics stats)
                        .Where(x => x.Name == "Doe")
                        .AggregateBy(x => x.ByField(y => y.Name).SumOn(y => y.Count))

                        .Execute();

                    Assert.NotNull(stats.IndexName);
                }

                using (var session = store.OpenAsyncSession())
                {
                    await session
                        .Query<User, UsersByName>()
                        .Statistics(out QueryStatistics stats)
                        .AggregateBy(x => x.ByField(y => y.Name).SumOn(y => y.Count))
                        .ExecuteAsync();
                    Assert.NotNull(stats.IndexName);
                }

                using (var session = store.OpenSession())
                {
                    var query = session
                        .Advanced
                        .DocumentQuery<User, UsersByName>();

                    query.Statistics(out var stats);

                    query.AggregateBy(x => x.ByField(y => y.Name).SumOn(y => y.Count)).Execute();

                    Assert.NotNull(stats.IndexName);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = session
                        .Advanced
                        .AsyncDocumentQuery<User, UsersByName>();

                    query.Statistics(out var stats);

                    await query.AggregateBy(x => x.ByField(y => y.Name).SumOn(y => y.Count)).ExecuteAsync();

                    Assert.NotNull(stats.IndexName);

                }
            }
        }

        [Fact]
        public async Task CanGetValidStatisticsInSuggestionQuery()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new UsersByName());

                using (var session = store.OpenSession())
                {
                    session
                        .Query<User, UsersByName>()
                        .Statistics(out QueryStatistics stats)
                        .SuggestUsing(x => x.ByField(y => y.Name, "Orin"))
                        .Execute();

                    Assert.NotNull(stats.IndexName);
                }

                using (var session = store.OpenAsyncSession())
                {
                    await session
                        .Query<User, UsersByName>()
                        .Statistics(out QueryStatistics stats)
                        .SuggestUsing(x => x.ByField(y => y.Name, "Orin"))
                        .ExecuteAsync();

                    Assert.NotNull(stats.IndexName);
                }

                using (var session = store.OpenSession())
                {
                    var query = session
                        .Advanced
                        .DocumentQuery<User, UsersByName>();

                    query.Statistics(out QueryStatistics stats);

                    query.SuggestUsing(x => x.ByField(y => y.Name, "Orin"))
                        .Execute();

                    Assert.NotNull(stats.IndexName);

                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = session
                        .Advanced
                        .AsyncDocumentQuery<User, UsersByName>();

                    query.Statistics(out QueryStatistics stats);

                    await query.SuggestUsing(x => x.ByField(y => y.Name, "Orin"))
                        .ExecuteAsync();

                    Assert.NotNull(stats.IndexName);
                }
            }
        }

        private class UsersByName : AbstractIndexCreationTask<User>
        {
            public UsersByName()
            {
                Map = users => from user in users
                    select new
                    {
                        FirstName = user.Name,
                        user.LastName
                    };

                Suggestion(x => x.Name);
            }
        }
    }
}
