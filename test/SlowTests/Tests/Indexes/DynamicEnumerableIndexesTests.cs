using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Indexes
{
    public class DynamicEnumerableIndexesTests :RavenTestBase
    {
        public DynamicEnumerableIndexesTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithFirstOrDefaultOnWhere(Options options)
        {
            IndexWithDynamicSource<FirstOrDefaultOnWhere_Index>(options);
        }

        public class FirstOrDefaultOnWhere_Index : AbstractIndexCreationTask<Item>
        {
            public FirstOrDefaultOnWhere_Index()
            {
                Map = items => from item in items
                    let node = item.Nodes.Where(x => x.Key == item.Id).FirstOrDefault()
                    select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithFirstOrDefaultOnWhereWithPredicate(Options options)
        {
            IndexWithDynamicSource<FirstOrDefaultOnWhereWithPredicate_Index>(options);
        }
        public class FirstOrDefaultOnWhereWithPredicate_Index : AbstractIndexCreationTask<Item>
        {
            public FirstOrDefaultOnWhereWithPredicate_Index()
            {
                Map = items => from item in items
                    let node = item.Nodes.Where(x => x.Key == item.Id).FirstOrDefault(x => x.Value == "sample")
                    select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithFirstOnWhere(Options options)
        {
            IndexWithDynamicSource<FirstOnWhere_Index>(options);
        }
        public class FirstOnWhere_Index : AbstractIndexCreationTask<Item>
        {
            public FirstOnWhere_Index()
            {
                Map = items => from item in items
                    let node = item.Nodes.Where(x => x.Key == item.Id).First()
                    select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithFirstOnWhereWithPredicate(Options options)
        {
            IndexWithDynamicSource<FirstOnWhereWithPredicate_Index>(options);
        }
        public class FirstOnWhereWithPredicate_Index : AbstractIndexCreationTask<Item>
        {
            public FirstOnWhereWithPredicate_Index()
            {
                Map = items => from item in items
                    let node = item.Nodes.Where(x => x.Key == item.Id).First(x => x.Value == "sample")
                    select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithSingleOrDefaultOnWhere(Options options)
        {
            IndexWithDynamicSource<SingleOrDefaultOnWhere_Index>(options);
        }
        public class SingleOrDefaultOnWhere_Index : AbstractIndexCreationTask<Item>
        {
            public SingleOrDefaultOnWhere_Index()
            {
                Map = items => from item in items
                    let node = item.Nodes.Where(x => x.Key == item.Id).SingleOrDefault()
                    select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithSingleOrDefaultOnWhereWithPredicate(Options options)
        {
            IndexWithDynamicSource<SingleOrDefaultOnWhereWithPredicate_Index>(options);
        }
        public class SingleOrDefaultOnWhereWithPredicate_Index : AbstractIndexCreationTask<Item>
        {
            public SingleOrDefaultOnWhereWithPredicate_Index()
            {
                Map = items => from item in items
                    let node = item.Nodes.Where(x => x.Key == item.Id).SingleOrDefault(x => x.Value == "sample")
                    select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithSingleOnWhere(Options options)
        {
            IndexWithDynamicSource<SingleOnWhere_Index>(options);
        }
        public class SingleOnWhere_Index : AbstractIndexCreationTask<Item>
        {
            public SingleOnWhere_Index()
            {
                Map = items => from item in items
                    let node = item.Nodes.Where(x => x.Key == item.Id).Single()
                    select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithSingleOnWhereWithPredicate(Options options)
        {
            IndexWithDynamicSource<SingleOnWhereWithPredicate_Index>(options);
        }
        public class SingleOnWhereWithPredicate_Index : AbstractIndexCreationTask<Item>
        {
            public SingleOnWhereWithPredicate_Index()
            {
                Map = items => from item in items
                    let node = item.Nodes.Where(x => x.Key == item.Id).Single(x => x.Value == "sample")
                    select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithLastOrDefaultOnWhere(Options options)
        {
            IndexWithDynamicSource<LastOrDefaultOnWhere_Index>(options);
        }
        public class LastOrDefaultOnWhere_Index : AbstractIndexCreationTask<Item>
        {
            public LastOrDefaultOnWhere_Index()
            {
                Map = items => from item in items
                    let node = item.Nodes.Where(x => x.Key == item.Id).LastOrDefault()
                    select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }
        
        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithLastOrDefaultOnWhereWithPredicate(Options options)
        {
            IndexWithDynamicSource<LastOrDefaultOnWhereWithPredicate_Index>(options);
        }
        public class LastOrDefaultOnWhereWithPredicate_Index : AbstractIndexCreationTask<Item>
        {
            public LastOrDefaultOnWhereWithPredicate_Index()
            {
                Map = items => from item in items
                    let node = item.Nodes.Where(x => x.Key == item.Id).LastOrDefault(x => x.Value == "sample")
                    select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithLastOnWhere(Options options)
        {
            IndexWithDynamicSource<LastOnWhere_Index>(options);
        }
        public class LastOnWhere_Index : AbstractIndexCreationTask<Item>
        {
            public LastOnWhere_Index()
            {
                Map = items => from item in items
                    let node = item.Nodes.Where(x => x.Key == item.Id).Last()
                    select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }


        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IndexWithLastOnWhereWithPredicate(Options options)
        {
            IndexWithDynamicSource<LastOnWhereWithPredicate_Index>(options);
        }
        public class LastOnWhereWithPredicate_Index : AbstractIndexCreationTask<Item>
        {
            public LastOnWhereWithPredicate_Index()
            {
                Map = items => from item in items
                    let node = item.Nodes.Where(x => x.Key == item.Id).Last(x => x.Value == "sample")
                    select new { Path = node.Value };

                Store("Path", FieldStorage.Yes);
            }
        }

        public void IndexWithDynamicSource<T>(Options options) where T : AbstractIndexCreationTask<Item>, new()
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

                Indexes.WaitForIndexing(store);
                WaitForUserToContinueTheTest(store);
                var errors = Indexes.WaitForIndexingErrors(store, new[] { index.IndexName }, errorsShouldExists: false)?
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
                    var query = session.Query<Result, T>().Select(x => x.Path).ToList();
                    switch (typeof(T))
                    {
                        case var i1 when i1 == typeof(FirstOnWhere_Index):
                        case var i2 when i2 == typeof(FirstOrDefaultOnWhere_Index):
                        case var i3 when i3 == typeof(LastOnWhere_Index):
                        case var i4 when i4 == typeof(LastOrDefaultOnWhere_Index):
                        case var i5 when i5 == typeof(SingleOnWhere_Index):
                        case var i6 when i6 == typeof(SingleOrDefaultOnWhere_Index):
                            Assert.Equal(1, query.Count);
                            Assert.Equal(query[0], "notsample1");
                            break;

                        case var i7 when i7 == typeof(LastOnWhereWithPredicate_Index):
                        case var i8 when i8 == typeof(LastOrDefaultOnWhereWithPredicate_Index):
                        case var i9 when i9 == typeof(FirstOnWhereWithPredicate_Index):
                        case var i10 when i10 == typeof(FirstOrDefaultOnWhereWithPredicate_Index):
                        case var i11 when i11 == typeof(SingleOnWhereWithPredicate_Index):
                        case var i12 when i12 == typeof(SingleOrDefaultOnWhereWithPredicate_Index):
                            Assert.Equal(0, query.Count);
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
