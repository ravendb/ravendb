using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.Linq;
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

        [RavenFact(RavenTestCategory.Indexes)]
        public void DisposingDynamicArrayEnumeratorDisposesWrappedEnumerator()
        {
            var source = new DisposableEnumerable();
            var array = new DynamicArray(source);

            using (var enumerator = array.GetEnumerator())
                Assert.True(enumerator.MoveNext());

            Assert.Equal(1, source.EnumeratorCount);
            Assert.Equal(1, source.DisposeCount);
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void MinAndMaxDisposeTheirEnumerators()
        {
            var minSource = new DisposableEnumerable();
            Assert.Equal(1L, DynamicEnumerable.Min(minSource));
            Assert.Equal(1, minSource.EnumeratorCount);
            Assert.Equal(1, minSource.DisposeCount);

            var maxSource = new DisposableEnumerable();
            Assert.Equal(1L, DynamicEnumerable.Max(maxSource.Cast<object>()));
            Assert.Equal(1, maxSource.EnumeratorCount);
            Assert.Equal(1, maxSource.DisposeCount);
        }

        private sealed class DisposableEnumerable : IEnumerable<object>
        {
            public int EnumeratorCount { get; private set; }

            public int DisposeCount { get; private set; }

            public IEnumerator<object> GetEnumerator()
            {
                EnumeratorCount++;
                return new DisposableEnumerator(this);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            private sealed class DisposableEnumerator : IEnumerator<object>
            {
                private readonly DisposableEnumerable _owner;
                private bool _moved;

                public DisposableEnumerator(DisposableEnumerable owner)
                {
                    _owner = owner;
                }

                public bool MoveNext()
                {
                    if (_moved)
                        return false;

                    _moved = true;
                    Current = 1L;
                    return true;
                }

                public void Reset()
                {
                    _moved = false;
                }

                public object Current { get; private set; }

                object IEnumerator.Current => Current;

                public void Dispose()
                {
                    _owner.DisposeCount++;
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void OrderByWithComparerCanBeFollowedByAdditionalOrderingLevels(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new OrderByWithComparerIndex();
                store.ExecuteIndex(index);

                var definition = store.Maintenance.Send(new GetIndexOperation(index.IndexName));
                var map = Assert.Single(definition.Maps);
                Assert.Contains(".OrderBy(", map);
                Assert.Contains(".ThenByDescending(", map);
                Assert.Contains(".ThenBy(", map);
                Assert.DoesNotContain("Enumerable.OrderBy", map);
                Assert.Contains("StringComparer.OrdinalIgnoreCase", map);

                using (var session = store.OpenSession())
                {
                    session.Store(new PrimitiveArrayItem { Tags = new[] { "a", "A" } });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, allowErrors: true);

                var errors = store.Maintenance
                    .Send(new GetIndexErrorsOperation(new[] { index.IndexName }))
                    .Single()
                    .Errors;
                Assert.Empty(errors);

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<OrderByWithComparerIndex.Result>(
                            $"from index '{index.IndexName}' select First, NullComparerFirst, NullDescendingComparerFirst")
                        .Single();
                    Assert.Equal("A", result.First);
                    Assert.Equal("a", result.NullComparerFirst);
                    Assert.Equal("a", result.NullDescendingComparerFirst);
                }
            }
        }

        public class OrderByWithComparerIndex : AbstractIndexCreationTask<PrimitiveArrayItem, OrderByWithComparerIndex.Result>
        {
            public OrderByWithComparerIndex()
            {
                Map = items => from item in items
                               select new Result
                               {
                                   First = item.Tags
                                       .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
                                       .ThenByDescending(x => x.Length)
                                       .ThenBy(x => x.ToString() == "A" ? 0 : 1)
                                       .First(),
                                   NullComparerFirst = item.Tags
                                       .OrderBy(x => x.Length)
                                       .ThenBy(x => x == "a" ? 0 : 1, null)
                                       .First(),
                                   NullDescendingComparerFirst = item.Tags
                                       .OrderBy(x => x.Length)
                                       .ThenByDescending(x => x == "a" ? 1 : 0, null)
                                       .First()
                               };

                Store(x => x.First, FieldStorage.Yes);
                Store(x => x.NullComparerFirst, FieldStorage.Yes);
                Store(x => x.NullDescendingComparerFirst, FieldStorage.Yes);
            }

            public class Result
            {
                public string First { get; set; }

                public string NullComparerFirst { get; set; }

                public string NullDescendingComparerFirst { get; set; }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void PrimitiveArrayLinqOperationsRunWithReleasedDefinitionSpelling(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new PrimitiveArrayLinqOperationsIndex();
                store.ExecuteIndex(index);

                var definition = store.Maintenance.Send(new GetIndexOperation(index.IndexName));
                var map = Assert.Single(definition.Maps);
                foreach (var method in new[] { "Contains", "SequenceEqual" })
                {
                    Assert.Contains($"Enumerable.{method}", map);
                    Assert.DoesNotContain($"DynamicEnumerable.{method}", map);
                }

                Assert.Contains("DynamicEnumerable.Concat", map);

                using (var session = store.OpenSession())
                {
                    session.Store(new PrimitiveArrayItem
                    {
                        Values = new[] { 1L, 2L, 3L },
                        Tags = new[] { "a", "b" }
                    });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, allowErrors: true);

                var errors = store.Maintenance
                    .Send(new GetIndexErrorsOperation(new[] { index.IndexName }))
                    .Single()
                    .Errors;
                Assert.Empty(errors);

                using (var session = store.OpenSession())
                {
                    var count = session.Advanced.RawQuery<dynamic>(
                            $"from index '{index.IndexName}' " +
                            "where ContainsTwo = true and ContainsCaseInsensitive = true and ExactSequence = true " +
                            "and ConcatCount = 4")
                        .Count();
                    Assert.Equal(1, count);
                }
            }
        }

        public class PrimitiveArrayLinqOperationsIndex : AbstractIndexCreationTask<PrimitiveArrayItem, PrimitiveArrayLinqOperationsIndex.Result>
        {
            public PrimitiveArrayLinqOperationsIndex()
            {
                Map = items => from item in items
                               select new Result
                               {
                                   ContainsTwo = item.Values.Contains(2L),
                                   ContainsCaseInsensitive = item.Tags.Contains("A", StringComparer.OrdinalIgnoreCase),
                                   ExactSequence = item.Values.SequenceEqual(new[] { 1L, 2L, 3L }),
                                   ConcatCount = item.Values.Concat(new[] { 4L }).Count()
                               };
            }

            public class Result
            {
                public bool ContainsTwo { get; set; }

                public bool ContainsCaseInsensitive { get; set; }

                public bool ExactSequence { get; set; }

                public int ConcatCount { get; set; }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void SequenceEqualOnLongArrayUsesUnchangedDefinition(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new SequenceEqualOnLongArrayIndex();
                store.ExecuteIndex(index);

                var definition = store.Maintenance.Send(new GetIndexOperation(index.IndexName));
                var map = Assert.Single(definition.Maps);
                Assert.Contains("Enumerable.SequenceEqual", map);
                Assert.DoesNotContain("DynamicEnumerable.SequenceEqual", map);

                using (var session = store.OpenSession())
                {
                    session.Store(new PrimitiveArrayItem { Values = new[] { 1L, 2L, 3L } });
                    session.Store(new PrimitiveArrayItem { Values = new[] { 1L, 2L } });
                    session.Store(new PrimitiveArrayItem { Values = new[] { 2L, 3L, 4L } });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, allowErrors: true);

                var errors = store.Maintenance
                    .Send(new GetIndexErrorsOperation(new[] { index.IndexName }))
                    .Single()
                    .Errors;
                Assert.Empty(errors);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<SequenceEqualOnLongArrayIndex.Result, SequenceEqualOnLongArrayIndex>()
                        .Where(x => x.IsExact)
                        .ToList();
                    Assert.Single(results);
                }
            }
        }

        public class SequenceEqualOnLongArrayIndex : AbstractIndexCreationTask<PrimitiveArrayItem, SequenceEqualOnLongArrayIndex.Result>
        {
            public SequenceEqualOnLongArrayIndex()
            {
                Map = items => from item in items
                               select new Result
                               {
                                   IsExact = item.Values.SequenceEqual(new[] { 1L, 2L, 3L })
                               };
            }

            public class Result
            {
                public bool IsExact { get; set; }
            }
        }

        public class PrimitiveArrayItem
        {
            public string Group { get; set; }

            public long[] Values { get; set; }

            public string[] Tags { get; set; }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void SequenceEqualOnLongArrayWorksInReduce(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new SequenceEqualOnLongArrayMapReduceIndex();
                store.ExecuteIndex(index);

                var definition = store.Maintenance.Send(new GetIndexOperation(index.IndexName));
                Assert.Contains("Enumerable.SequenceEqual", definition.Reduce);
                Assert.DoesNotContain("DynamicEnumerable.SequenceEqual", definition.Reduce);

                using (var session = store.OpenSession())
                {
                    session.Store(new PrimitiveArrayItem
                    {
                        Group = "expected",
                        Values = new[] { 1L, 2L, 3L }
                    });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, allowErrors: true);

                var errors = store.Maintenance
                    .Send(new GetIndexErrorsOperation(new[] { index.IndexName }))
                    .Single()
                    .Errors;
                Assert.Empty(errors);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<SequenceEqualOnLongArrayMapReduceIndex.Result, SequenceEqualOnLongArrayMapReduceIndex>()
                        .Where(x => x.IsExact)
                        .ToList();
                    Assert.Single(results);
                }
            }
        }

        public class SequenceEqualOnLongArrayMapReduceIndex : AbstractIndexCreationTask<PrimitiveArrayItem, SequenceEqualOnLongArrayMapReduceIndex.Result>
        {
            public SequenceEqualOnLongArrayMapReduceIndex()
            {
                Map = items => from item in items
                               select new Result
                               {
                                   Group = item.Group,
                                   Values = item.Values,
                                   IsExact = false
                               };

                Reduce = results => from result in results
                                    group result by result.Group
                                    into grouped
                                    let values = grouped.First().Values
                                    select new Result
                                    {
                                        Group = grouped.Key,
                                        Values = values,
                                        IsExact = values.SequenceEqual(new[] { 1L, 2L, 3L })
                                    };
            }

            public class Result
            {
                public string Group { get; set; }

                public long[] Values { get; set; }

                public bool IsExact { get; set; }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
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

        [RavenTheory(RavenTestCategory.Indexes)]
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

        [RavenTheory(RavenTestCategory.Indexes)]
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

        [RavenTheory(RavenTestCategory.Indexes)]
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

        [RavenTheory(RavenTestCategory.Indexes)]
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

        [RavenTheory(RavenTestCategory.Indexes)]
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

        [RavenTheory(RavenTestCategory.Indexes)]
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

        [RavenTheory(RavenTestCategory.Indexes)]
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

        [RavenTheory(RavenTestCategory.Indexes)]
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

        [RavenTheory(RavenTestCategory.Indexes)]
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

        [RavenTheory(RavenTestCategory.Indexes)]
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


        [RavenTheory(RavenTestCategory.Indexes)]
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
