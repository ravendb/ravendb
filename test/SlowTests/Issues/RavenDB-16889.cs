using System.Collections;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16889 : RavenTestBase
    {
        public RavenDB_16889(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void SelectManySimplePredicate()
        {
            TestCase<SelectManySimplePredicateIndex>();
        }
        private class SelectManySimplePredicateIndex : AbstractIndexCreationTask<TestObj>
        {
            public SelectManySimplePredicateIndex()
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

        [Fact]
        public void SelectManyComplexPredicate()
        {
            TestCase<SelectManyComplexPredicateIndex>();
        }
        private class SelectManyComplexPredicateIndex : AbstractIndexCreationTask<TestObj>
        {
            public SelectManyComplexPredicateIndex()
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

        [Fact]
        public void SelectManyComplexPredicate2Args()
        {
            TestCase<SelectManyComplexPredicate2ArgsIndex>();
        }
        private class SelectManyComplexPredicate2ArgsIndex : AbstractIndexCreationTask<TestObj>
        {
            public SelectManyComplexPredicate2ArgsIndex()
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

        [Fact]
        public void SelectManySimplePredicate2Args()
        {
            TestCase<SelectManySimplePredicate2ArgsIndex>();
        }

        private class SelectManySimplePredicate2ArgsIndex : AbstractIndexCreationTask<TestObj>
        {
            public SelectManySimplePredicate2ArgsIndex()
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

        private void TestCase<T>() where T : AbstractIndexCreationTask<TestObj>, new()
        {
            using var store = GetDocumentStore();
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

        [Fact]
        public void UserTest()
        {
            TestCase<TestIndex>();
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
