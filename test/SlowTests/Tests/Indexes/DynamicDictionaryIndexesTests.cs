using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Indexes
{
    public class DynamicDictionaryIndexesTests : RavenTestBase
    {
        public DynamicDictionaryIndexesTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithFirstOrDefaultOnDictionary(Options options)
        {
            IndexWithDynamicSource<FirstOrDefaultOnDictionary_Index>(options);
        }
        public class FirstOrDefaultOnDictionary_Index : AbstractIndexCreationTask<Item>
        {
            public FirstOrDefaultOnDictionary_Index()
            {
                Map = items => from item in items
                               let node = item.Nodes.FirstOrDefault()
                               select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithFirstOrDefaultOnDictionaryWithPredicate(Options options)
        {
            IndexWithDynamicSource<FirstOrDefaultOnDictionaryWithPredicate_Index>(options);
        }
        public class FirstOrDefaultOnDictionaryWithPredicate_Index : AbstractIndexCreationTask<Item>
        {
            public FirstOrDefaultOnDictionaryWithPredicate_Index()
            {
                Map = items => from item in items
                               let node = item.Nodes.FirstOrDefault(x => x.Key == item.Id)
                               select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithFirstOnDictionary(Options options)
        {
            IndexWithDynamicSource<FirstOnDictionary_Index>(options);
        }
        public class FirstOnDictionary_Index : AbstractIndexCreationTask<Item>
        {
            public FirstOnDictionary_Index()
            {
                Map = items => from item in items
                               let node = item.Nodes.First()
                               select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithFirstOnDictionaryWithPredicate(Options options)
        {
            IndexWithDynamicSource<FirstOnDictionaryWithPredicate_Index>(options);
        }
        public class FirstOnDictionaryWithPredicate_Index : AbstractIndexCreationTask<Item>
        {
            public FirstOnDictionaryWithPredicate_Index()
            {
                Map = items => from item in items
                               let node = item.Nodes.First(x => x.Key == item.Id)
                               select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithSingleOrDefaultOnDictionary(Options options)
        {
            IndexWithDynamicSource<SingleOrDefaultOnDictionary_Index>(options, waitForErrors: true);
        }
        public class SingleOrDefaultOnDictionary_Index : AbstractIndexCreationTask<Item>
        {
            public SingleOrDefaultOnDictionary_Index()
            {
                Map = items => from item in items
                               let node = item.Nodes.SingleOrDefault()
                               select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithSingleOrDefaultOnDictionaryWithPredicate(Options options)
        {
            IndexWithDynamicSource<SingleOrDefaultOnDictionaryWithPredicate_Index>(options);
        }
        public class SingleOrDefaultOnDictionaryWithPredicate_Index : AbstractIndexCreationTask<Item>
        {
            public SingleOrDefaultOnDictionaryWithPredicate_Index()
            {
                Map = items => from item in items
                               let node = item.Nodes.SingleOrDefault(x => x.Key == item.Id)
                               select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithSingleOnDictionary(Options options)
        {
            IndexWithDynamicSource<SingleOnDictionary_Index>(options, waitForErrors: true);
        }
        public class SingleOnDictionary_Index : AbstractIndexCreationTask<Item>
        {
            public SingleOnDictionary_Index()
            {
                Map = items => from item in items
                               let node = item.Nodes.Single()
                               select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithSingleOnDictionaryWithPredicate(Options options)
        {
            IndexWithDynamicSource<SingleOnDictionaryWithPredicate_Index>(options);
        }
        public class SingleOnDictionaryWithPredicate_Index : AbstractIndexCreationTask<Item>
        {
            public SingleOnDictionaryWithPredicate_Index()
            {
                Map = items => from item in items
                               let node = item.Nodes.Single(x => x.Key == item.Id)
                               select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithLastOrDefaultOnDictionary(Options options)
        {
            IndexWithDynamicSource<LastOrDefaultOnDictionary_Index>(options);
        }
        public class LastOrDefaultOnDictionary_Index : AbstractIndexCreationTask<Item>
        {
            public LastOrDefaultOnDictionary_Index()
            {
                Map = items => from item in items
                               let node = item.Nodes.Last()
                               select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithLastOrDefaultOnDictionaryWithPredicate(Options options)
        {
            IndexWithDynamicSource<LastOrDefaultOnDictionaryWithPredicate_Index>(options);
        }
        public class LastOrDefaultOnDictionaryWithPredicate_Index : AbstractIndexCreationTask<Item>
        {
            public LastOrDefaultOnDictionaryWithPredicate_Index()
            {
                Map = items => from item in items
                               let node = item.Nodes.LastOrDefault(x => x.Key == item.Id)
                               select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithLastOnDictionary(Options options)
        {
            IndexWithDynamicSource<LastOnDictionary_Index>(options);
        }
        public class LastOnDictionary_Index : AbstractIndexCreationTask<Item>
        {
            public LastOnDictionary_Index()
            {
                Map = items => from item in items
                               let node = item.Nodes.Last()
                               select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithLastOnDictionaryWithPredicate(Options options)
        {
            IndexWithDynamicSource<LastOnDictionaryWithPredicate_Index>(options);
        }
        public class LastOnDictionaryWithPredicate_Index : AbstractIndexCreationTask<Item>
        {
            public LastOnDictionaryWithPredicate_Index()
            {
                Map = items => from item in items
                               let node = item.Nodes.Last(x => x.Key == item.Id)
                               select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        public void IndexWithDynamicSource<T>(Options options, bool waitForErrors = false) where T : AbstractIndexCreationTask<Item>, new()
        {
            var index = new T();
            using (var store = GetDocumentStore(options))
            {
                store.ExecuteIndex(index);

                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Id = "Items/1", Nodes = new Dictionary<string, string> { { "Items/1", "notsample1" }, { "Items/2", "notsample2" } } });
                    session.SaveChanges();
                }

                string[] errors;
                if (waitForErrors == false)
                {
                    Indexes.WaitForIndexing(store);

                    errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { index.IndexName }))
                        .SelectMany(e => e.Errors)
                        .Select(e => e.Error)
                        .ToArray();
                }
                else
                {
                    errors = Indexes.WaitForIndexingErrors(store, new[] { index.IndexName })
                        .SelectMany(e => e.Errors)
                        .Select(e => e.Error)
                        .ToArray();
                }

                var errorsString = string.Join("\n", errors);

                using (var session = store.OpenSession())
                {

                    switch (typeof(T))
                    {
                        case var i1 when i1 == typeof(FirstOnDictionaryWithPredicate_Index):
                        case var i2 when i2 == typeof(FirstOrDefaultOnDictionaryWithPredicate_Index):
                        case var i3 when i3 == typeof(LastOnDictionaryWithPredicate_Index):
                        case var i4 when i4 == typeof(LastOrDefaultOnDictionaryWithPredicate_Index):
                        case var i5 when i5 == typeof(SingleOnDictionaryWithPredicate_Index):
                        case var i6 when i6 == typeof(SingleOrDefaultOnDictionaryWithPredicate_Index):
                        case var i7 when i7 == typeof(FirstOnDictionary_Index):
                        case var i8 when i8 == typeof(FirstOrDefaultOnDictionary_Index):
                            var q = session.Query<Result, T>().Select(x => x.Path).ToList();
                            Assert.DoesNotContain("Failed to execute mapping function", errorsString);
                            Assert.Equal(1, q.Count);
                            Assert.Equal(q[0], "notsample1");
                            break;
                        case var i9 when i9 == typeof(LastOnDictionary_Index):
                        case var i10 when i10 == typeof(LastOrDefaultOnDictionary_Index):
                            var q2 = session.Query<Result, T>().Select(x => x.Path).ToList();
                            Assert.DoesNotContain("Failed to execute mapping function", errorsString);
                            Assert.Equal(1, q2.Count);
                            Assert.Equal(q2[0], "notsample2");
                            break;

                        case var i11 when i11 == typeof(SingleOnDictionary_Index):
                        case var i12 when i12 == typeof(SingleOrDefaultOnDictionary_Index):
                            Assert.Contains("Sequence contains more than one element", errorsString);
                            break;
                    }
                }
            }
        }
        public class Result
        {
            public string Path { get; set; }
        }

        public class Item
        {
            public string Id { get; set; }
            public Dictionary<string, string> Nodes { get; set; }
        }

    }
}
