using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14986 : RavenTestBase
    {
        public RavenDB_14986(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanSetFieldStorageNoAndFieldIndexingNoInMapReduce()
        {
            using (var store = GetDocumentStore())
            {
                new SimpleMapIndex().Execute(store);
                new SimpleMapReduceIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 25; i++)
                        session.Store(new Company { Name = $"C_{i}", ExternalId = $"E_{i}" });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, allowErrors: true);

                var errors = Indexes.WaitForIndexingErrors(store);
                Assert.Equal(2, errors.Length);

                var simpleMapErrors = errors.Single(x => x.Name == new SimpleMapIndex().IndexName);
                Assert.Equal(25, simpleMapErrors.Errors.Length);
                Assert.True(simpleMapErrors.Errors.All(x => x.Error.Contains("it doesn't make sense to have a field that is neither indexed nor stored")));

                var simpleMapReduceErrors = errors.Single(x => x.Name == new SimpleMapReduceIndex().IndexName);
                Assert.Equal(0, simpleMapReduceErrors.Errors.Length);

                using (var session = store.OpenSession())
                {
                    var results = session
                        .Query<SimpleMapReduceIndex.Result, SimpleMapReduceIndex>()
                        .Customize(x => x.NoCaching())
                        .ToList();

                    Assert.Equal(25, results.Count);
                    Assert.True(results.All(x => x.ExternalIds != null && x.ExternalIds.Length == 1));

                    foreach (var result in results)
                    {
                        Assert.NotNull(result.ExternalIds);
                        Assert.Equal(1, result.ExternalIds.Length);
                        Assert.Equal(new[] { result.Name.Replace("C", "E") }, result.ExternalIds);
                    }
                }
            }
        }

        private class SimpleMapIndex : AbstractIndexCreationTask<Company>
        {
            public SimpleMapIndex()
            {
                Map = companies => from c in companies
                                   select new
                                   {
                                       c.Name,
                                       c.ExternalId
                                   };

                Store(x => x.ExternalId, FieldStorage.No);
                Index(x => x.ExternalId, FieldIndexing.No);
            }
        }

        private class SimpleMapReduceIndex : AbstractIndexCreationTask<Company, SimpleMapReduceIndex.Result>
        {
            public class Result
            {
                public string Name { get; set; }

                public string[] ExternalIds { get; set; }

                public int Count { get; set; }
            }

            public SimpleMapReduceIndex()
            {
                Map = companies => from c in companies
                                   select new Result
                                   {
                                       Name = c.Name,
                                       ExternalIds = new[] { c.ExternalId },
                                       Count = 1
                                   };

                Reduce = results => from r in results
                                    group r by r.Name into g
                                    select new Result
                                    {
                                        Name = g.Key,
                                        ExternalIds = g.SelectMany(x => x.ExternalIds).ToArray(),
                                        Count = g.Sum(x => x.Count)
                                    };

                Store(x => x.ExternalIds, FieldStorage.No);
                Index(x => x.ExternalIds, FieldIndexing.No);
            }
        }
    }
}
