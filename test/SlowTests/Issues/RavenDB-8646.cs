using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Sharding;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDb8646 : RavenTestBase
    {
        public RavenDb8646(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void CanStreamQueryWithMapReduceResult(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new ReduceMeByTag();
                store.ExecuteIndex(index);
                using (var session = store.OpenSession())
                {
                    session.Store(new ReduceMe
                    {
                        Tag = "Foo",
                        Amount = 2
                    });
                    session.Store(new ReduceMe
                    {
                        Tag = "Foo",
                        Amount = 3
                    });
                    session.SaveChanges();
                    Indexes.WaitForIndexing(store);
                    var query = session.Query<ReduceMe>(index.IndexName);
                    using (var stream = session.Advanced.Stream(query))
                    {
                        var streamNotEmpty = false;
                        while (stream.MoveNext())
                        {
                            var current = stream.Current;
                            Assert.Equal(5, current.Document.Amount);
                            streamNotEmpty = true;
                        }
                        Assert.True(streamNotEmpty, "Was expecting to have a result in the query stream but didn't get anything.");
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
        public void CanStreamShardedQueryWithMapReduceResult(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new ReduceMeByTag();
                store.ExecuteIndex(index);
                using (var session = store.OpenSession())
                {
                    session.Store(new ReduceMe
                    {
                        Tag = "Foo",
                        Amount = 2
                    });
                    session.Store(new ReduceMe
                    {
                        Tag = "Foo",
                        Amount = 3
                    });
                    session.SaveChanges();
                    Indexes.WaitForIndexing(store);
                    var query = session.Query<ReduceMe>(index.IndexName);
                    var notSupportedException = Assert.Throws<NotSupportedInShardingException>(() =>
                    {
                        using (var stream = session.Advanced.Stream(query))
                        {
                            while (stream.MoveNext())
                            {
                            }
                        }
                    });

                    Assert.Contains("MapReduce is not supported in sharded streaming queries", notSupportedException.Message);
                }
            }
        }

        public class ReduceMe
        {
            public string Tag { get; set; }
            public int Amount { get; set; }
        }

        public class ReduceMeByTag : AbstractIndexCreationTask<ReduceMe>
        {
            public ReduceMeByTag()
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Tag,
                        doc.Amount
                    };
                Reduce = results => from r in results
                    group r by r.Tag
                    into g
                    select new
                    {
                        Tag = g.Key,
                        Amount = g.Sum(x => x.Amount)
                    };
            }
        }
    }
}
