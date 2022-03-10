using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using FastTests;
using FastTests.Server.Documents.Indexing;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Linq.Indexing;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Queries.Suggestions;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax
{
    public class CoraxSlowQueryTests : RavenTestBase
    {
        public CoraxSlowQueryTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenExplicitData(searchEngine: RavenSearchEngineMode.Corax)]
        public void CompoundOrderByWithPagination(RavenTestParameters config, int size = 10_000)
        {
            List<Result> expected = new();
            var option = new Options()
            {
                ModifyDatabaseRecord = d =>
                {
                    d.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = config.SearchEngine.ToString();
                    d.Settings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)] = config.SearchEngine.ToString();
                    d.Client = new ClientConfiguration() {MaxNumberOfRequestsPerSession = int.MaxValue};
                }
            };

            using (var store = GetDocumentStore(option))
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 0; i < size; ++i)
                    {
                        bulkInsert.Store(new Person() {Name = $"ItemNo{i}", Age = i % 100, Height = i % 200});
                        expected.Add(new Result() {Age = i % 100, Height = i % 200});
                    }
                }

                expected = expected.OrderBy(y => y.Age).ThenByDescending(y => y.Height).ToList();

                using (var session = store.OpenSession())
                {
                    List<Result> actual = new();
                    for (int i = 0; i < size; i += 70)
                        actual.AddRange(session.Query<Person>().OrderBy(y => y.Age).ThenByDescending(y => y.Height)
                            .Select(z => new Result() {Age = z.Age, Height = z.Height}).Skip(i).Take(70).ToList());

                    for (var i = 0; i < expected.Count; ++i)
                    {
                        Assert.Equal(expected[i].Age, actual[i].Age);
                        Assert.Equal(expected[i].Height, actual[i].Height);
                    }
                }
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
        public void AnalyzerAreApplied(Options options)
        {
            var item = new Person() {Name = "MaCiEJ"};
            using var store = GetDocumentStore(options);
            {
                using var session = store.OpenSession();
                session.Store(item);
                session.SaveChanges();
            }
            {
                using var session = store.OpenSession();
                var result = session.Query<Person>().Where(x => x.Name == "Maciej").SingleOrDefault();
                Assert.NotNull(result);
                Assert.Equal(item.Name, result.Name);
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
        public void BoostingTest(Options options)
        {
            const int terms = 29;
            using var coraxStore = GetDocumentStore(options);
            using var luceneStore = GetDocumentStore();
            List<Result> results;
            {
                using var coraxSession = coraxStore.BulkInsert();
                using var luceneSession = luceneStore.BulkInsert();
                results = Enumerable.Range(0, 10_000).Select(i => new Result() {Age = i % terms, Height = i}).ToList();
                results.ForEach((x) =>
                {
                    coraxSession.Store(x);
                    luceneSession.Store(x);
                });
            }


            {
                //TermMatches and BinaryMatches
                var rawQuery = new StringBuilder();
                rawQuery.Append("from Results where boost(Age == 0, 0)");
                for (int i = 1; i < terms; ++i)
                    rawQuery.Append($" or boost(Age == {i},{i})");
                rawQuery.Append(" order by score()");

                Assertion(rawQuery.ToString());
            }
            {
                //MultiTermMatches
                var rawQuery = new StringBuilder();
                rawQuery.Append("from Results where boost(startsWith(Age, \"0\"),0)");
                for (int i = 1; i < terms; ++i)
                    rawQuery.Append($" or boost(startsWith(Age, \"{i}\"),{i})");
                rawQuery.Append(" order by score()");

                Assertion(rawQuery.ToString());
            }

            {
                //UnaryTest
                WaitForUserToContinueTheTest(luceneStore);
                Assertion($"from Results where boost(Age > {terms - 2}, 100) order by score(), Age as alphanumeric desc ");
            }

            void Assertion(string rawQuery)
            {
                using var coraxSession = coraxStore.OpenSession();
                using var luceneSession = luceneStore.OpenSession();
                var luceneResult = luceneSession.Advanced.RawQuery<Result>(rawQuery.ToString()).ToList();

                var coraxResult = coraxSession.Advanced.RawQuery<Result>(rawQuery.ToString()).ToList();
                Assert.NotEmpty(luceneResult);
                Assert.NotEmpty(coraxResult);
                Assert.Equal(luceneResult.Count, coraxResult.Count);
                for (int i = 0; i < luceneResult.Count; ++i)
                    Assert.Equal(luceneResult[i].Age, coraxResult[i].Age);
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
        public void NgramSuggestionTest(Options options)
        {
            using (var documentStore = GetDocumentStore(options))
            {
                using (var s = documentStore.OpenSession())
                {
                    s.Store(new User {Name = "Maciej"});
                    s.Store(new User {Name = "Matt"});
                    s.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var suggestionQueryResult = session.Query<User>()
                        .SuggestUsing(x => x.ByField(y => y.Name, "Mett").WithOptions(new SuggestionOptions
                        {
                            PageSize = 10, Accuracy = 0.25f, Distance = StringDistanceTypes.NGram
                        }))
                        .Execute();

                    Assert.True(suggestionQueryResult["Name"].Suggestions.Count >= 1);
                    Assert.Contains("matt", suggestionQueryResult["Name"].Suggestions);
                }
            }
        }

        public class TestA
        {
            public string Name { get; set; }
            public TestB Inner { get; set; }
        }

        public class TestB
        {
            public string InnerName { get; set; }
            public TestC Inner { get; set; }
        }

        public class TestC
        {
            public string InnerName { get; set; }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void TestOnDeep(Options options)
        {
            using var store = GetDocumentStore(options);
            {
                var s = store.OpenSession();
                s.Store(
                    new TestA() {Name = "Matt", Inner = new() {InnerName = "Jan", Inner = new() {InnerName = "Tester"}}});
                s.SaveChanges();
            }

            {
                var s = store.OpenSession();

                var result = s.Query<TestA>().Where(a => a.Inner == new TestB() {InnerName = "Jan", Inner = new() {InnerName = "Tester"}}).ToList()
                    ;
                Assert.Equal(1, result.Count);
            }


            WaitForUserToContinueTheTest(store);
        }


        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
        public void MaxSuggestionsShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                using (var session = store.OpenSession())
                {
                    var result = session
                        .Query<Product>()
                        .SuggestUsing(f => f.ByField("Name", new[] {"chaig", "tof"}).WithOptions(new SuggestionOptions
                        {
                            PageSize = 5, Distance = StringDistanceTypes.NGram, SortMode = SuggestionSortMode.Popularity, Accuracy = 0.5f
                        }))
                        .Execute();

                    Assert.True(result["Name"].Suggestions.Count is > 0 and <= 5);
                }
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
        public void CanSearchOnLists(Options options)
        {
            using var store = GetDocumentStore(options);
            {
                using var s = store.OpenSession();
                s.Store(new ListOfNames(new []{"Maciej", "Gracjan", "Marcin", "Arek", "Tomek", "Pawel"}));
                s.Store(new ListOfNames(new []{"Lukasz", "Damian", "Grzesiu", "Bartek", "Oliwia"}));
                s.Store(new ListOfNames(new []{"Krzysiu", "Rafal", "Mateusz"}));

                s.SaveChanges();
            }
            {
                using var s = store.OpenSession();
                var r = s.Query<ListOfNames>().Search(p => p.Names, "maciej").ToList();
                Assert.Equal(1, r.Count);
                
            }
        }

        private record ListOfNames(string[] Names);
        
        
        private class Result
        {
            public int Age { get; set; }
            public int Height { get; set; }
        }

        private class Person
        {
            public string Name { get; set; }
            public int Age { get; set; }
            public int Height { get; set; }
        }
    }
}
