using System.Collections;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16889 : RavenTestBase
    {
        public RavenDB_16889(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void SelectManySimplePredicate(Options options)
        {
            if (options.SearchEngineMode is RavenSearchEngineMode.Corax)
                TestCase<SelectManySimplePredicateIndexCorax>(options);
            else
                TestCase<SelectManySimplePredicateIndexLucene>(options);
        }
        private class SelectManySimplePredicateIndexCorax : AbstractIndexCreationTask<TestObj>
        {
            public SelectManySimplePredicateIndexCorax()
            {
                Map = taggables =>
                    from taggable in taggables
                    select new
                    {
                        TagsResult = taggable.Tags
                            .SelectMany((v => v.Value))
                    };
                Store("TagsResult", FieldStorage.Yes);
                Index("TagsResult", FieldIndexing.No);
            }
        }

        private class SelectManySimplePredicateIndexLucene : AbstractIndexCreationTask<TestObj>
        {
            public SelectManySimplePredicateIndexLucene()
            {
                Map = taggables =>
                    from taggable in taggables
                    select new
                    {
                        TagsResult = taggable.Tags
                            .SelectMany((v => v.Value))
                    };
                Store("TagsResult", FieldStorage.Yes);
            }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void SelectManyComplexPredicate(Options options)
        {
            if (options.SearchEngineMode is RavenSearchEngineMode.Corax)
                TestCase<SelectManyComplexPredicateIndexCorax>(options);
            else
                TestCase<SelectManyComplexPredicateIndexLucene>(options);
        }
        
        private class SelectManyComplexPredicateIndexLucene : AbstractIndexCreationTask<TestObj>
        {
            public SelectManyComplexPredicateIndexLucene()
            {
                Map = taggables =>
                    from taggable in taggables
                    select new
                    {
                        TagsResult = taggable.Tags
                            .SelectMany((v,i) => v.Value)
                    };
                Store("TagsResult", FieldStorage.Yes);
            }
        }
        
        private class SelectManyComplexPredicateIndexCorax : AbstractIndexCreationTask<TestObj>
        {
            public SelectManyComplexPredicateIndexCorax()
            {
                Map = taggables =>
                    from taggable in taggables
                    select new
                    {
                        TagsResult = taggable.Tags
                            .SelectMany((v,i) => v.Value)
                    };
                Store("TagsResult", FieldStorage.Yes);
                Index("TagsResult", FieldIndexing.No);
            }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void SelectManyComplexPredicate2Args(Options options)
        {
            if (options.SearchEngineMode is RavenSearchEngineMode.Corax)
                TestCase<SelectManyComplexPredicate2ArgsIndexCorax>(options);
            else
                TestCase<SelectManyComplexPredicate2ArgsIndexLucene>(options);
        }
        private class SelectManyComplexPredicate2ArgsIndexLucene : AbstractIndexCreationTask<TestObj>
        {
            public SelectManyComplexPredicate2ArgsIndexLucene()
            {
                Map = taggables =>
                    from taggable in taggables
                    select new
                    {
                        TagsResult = taggable.Tags
                            .SelectMany((v, i) => v.Value, (pair, valuePair) => valuePair)
                    };
                Store("TagsResult", FieldStorage.Yes);
            }
        }
        
        private class SelectManyComplexPredicate2ArgsIndexCorax : AbstractIndexCreationTask<TestObj>
        {
            public SelectManyComplexPredicate2ArgsIndexCorax()
            {
                Map = taggables =>
                    from taggable in taggables
                    select new
                    {
                        TagsResult = taggable.Tags
                            .SelectMany((v, i) => v.Value, (pair, valuePair) => valuePair)
                    };
                Store("TagsResult", FieldStorage.Yes);
                Index("TagsResult", FieldIndexing.No);
            }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void SelectManySimplePredicate2Args(Options options)
        {
            if (options.SearchEngineMode is RavenSearchEngineMode.Corax) 
                TestCase<SelectManySimplePredicate2ArgsIndexCorax>(options);
            else
                TestCase<SelectManySimplePredicate2ArgsIndexLucene>(options);
        }

        private class SelectManySimplePredicate2ArgsIndexLucene : AbstractIndexCreationTask<TestObj>
        {
            public SelectManySimplePredicate2ArgsIndexLucene()
            {
                Map = taggables =>
                    from taggable in taggables
                    select new
                    {
                        TagsResult = taggable.Tags
                            .SelectMany(v => v.Value, (pair, valuePair) => valuePair)
                    };
                Store("TagsResult", FieldStorage.Yes);
            }
        }
        
        private class SelectManySimplePredicate2ArgsIndexCorax : AbstractIndexCreationTask<TestObj>
        {
            public SelectManySimplePredicate2ArgsIndexCorax()
            {
                Map = taggables =>
                    from taggable in taggables
                    select new
                    {
                        TagsResult = taggable.Tags
                            .SelectMany(v => v.Value, (pair, valuePair) => valuePair)
                    };
                Store("TagsResult", FieldStorage.Yes);
                Index("TagsResult", FieldIndexing.No);
            }
        }

        private void TestCase<T>(Options options) where T : AbstractIndexCreationTask<TestObj>, new()
        {
            using var store = GetDocumentStore(options);
            var index = new T();
            index.Execute(store);

            var Tags = new Dictionary<string, Dictionary<string, string>>
            {
                {
                    "key1", new Dictionary<string, string>
                    {
                        {"key2", "value1"}
                    }
                }
            };

            using (var session = store.OpenSession())
            {
                session.Store(new TestObj
                {
                    Tags = Tags
                });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);
            
            var errors = Indexes.WaitForIndexingErrors(store, new []{index.IndexName}, errorsShouldExists: false)?
                .SelectMany(e => e.Errors)
                .Select(e => e.Error)
                .ToArray();
            if (errors is not null)
            {
                var errorsString = string.Join("\n", errors);
                Assert.DoesNotContain("Failed to execute mapping function", errorsString);
            }

            using (var session = store.OpenSession())
            {
                if (typeof(T) == typeof(TestIndex))
                {
                    List<int> query = session.Query<UserTestResult, T>().Select(x => x.Count).ToList();
                    Assert.Equal(1, query[0]);
                }
                else
                {
                    List<IDictionary> query = session.Query<Result, T>().Select(x => x.TagsResult).ToList();
                    Assert.Equal(1, query.Count);
                    Assert.Equal("value1", query[0]["key2"]);
                }
            }
            
            WaitForUserToContinueTheTest(store);
        }

        private class TestObj
        {
            public Dictionary<string, Dictionary<string, string>> Tags;
        }

        private class Result
        {
#pragma warning disable 649
            public IDictionary TagsResult;
#pragma warning restore 649
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void UserTest(Options options)
        {
            TestCase<TestIndex>(options);
        }

        private class TestIndex : AbstractIndexCreationTask<TestObj>
        {
            public TestIndex()
            {
                Map = taggables =>
                    from taggable in taggables
                    select new
                    {
                        Count = taggable.Tags
                            .SelectMany((v => v.Value))
                            .Count()
                    };
                Store("Count", FieldStorage.Yes);
            }
        }

        private class UserTestResult
        {
#pragma warning disable 649
            public int Count;
#pragma warning restore 649
        }
    }
}
