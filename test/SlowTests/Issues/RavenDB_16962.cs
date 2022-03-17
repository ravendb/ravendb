using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16962 : RavenTestBase
    {
        public RavenDB_16962(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void SelectWithComplexPredicate()
        {
            var index = new SelectWithComplexPredicateIndex();
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(index);

                using (var session = store.OpenSession())
                {
                    session.Store(new Response
                    {
                        Items = new[]
                        {
                            new Item { Value = "FirstItem" }, 
                            new Item { Value = "SecondItem" }
                        }
                    });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

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
                    var query = session.Query<SelectWithComplexPredicateIndex.Result, SelectWithComplexPredicateIndex>()
                        .ProjectInto<SelectWithComplexPredicateIndex.Result>()
                        .ToList();
                    Assert.Equal(2, query.Count);
                    Assert.Equal("FirstItem", query[(query[0].Index)].Value);
                    Assert.Equal("SecondItem", query[(query[1].Index)].Value);
                }
            }
        }

        [Fact]
        public void SelectWithSimplePredicate()
        {
            var index = new SelectWithSimplePredicateIndex();
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(index);

                using (var session = store.OpenSession())
                {
                    session.Store(new Response
                    {
                        Items = new[]
                        {
                            new Item { Value = "FirstItem" }, 
                            new Item { Value = "SecondItem" }
                        }
                    });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

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
                    var query = session.Query<SelectWithSimplePredicateIndex.Result, SelectWithSimplePredicateIndex>()
                        .ProjectInto<SelectWithSimplePredicateIndex.Result>()
                        .ToList();
                    Assert.Equal(2, query.Count);
                    Assert.Equal("FirstItem", query[0].Value);
                    Assert.Equal("SecondItem", query[1].Value);
                }
            }
        }

        private class SelectWithComplexPredicateIndex : AbstractIndexCreationTask<Response, SelectWithComplexPredicateIndex.Result>
        {
            public class Result
            {
                public string Value { get; set; }
                public int Index { get; set; }
            }
            public SelectWithComplexPredicateIndex()
            {
                Map = responses => from response in responses
                   from item in response.Items.Select((x, index) => new { Value = x.Value, Index = index })
                   select new Result
                   {
                       Value = item.Value,
                       Index = item.Index
                   };
                Store("Value", FieldStorage.Yes);
                Store("Index", FieldStorage.Yes);
            }
        }

        private class SelectWithSimplePredicateIndex : AbstractIndexCreationTask<Response, SelectWithSimplePredicateIndex.Result>
        {
            public class Result
            {
                public string Value { get; set; }
            }
            public SelectWithSimplePredicateIndex()
            {
                Map = responses => from response in responses
                    from item in response.Items.Select(x => new { Value = x.Value })
                    select new Result
                    {
                        Value = item.Value
                    };
                Store("Value", FieldStorage.Yes);
            }
        }

        private class Item
        {
            public string Value { get; set; }
        }
        private class Response
        {
            public Item[] Items { get; set; }
        }
    }
}
