using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDb8763 : RavenTestBase
    {
        [Fact]
        public void CanAsyncStreamQueryWithMapReduceResult()
        {
            using (var store = GetDocumentStore())
            {
                var index = new ReduceMeByTag();
                store.ExecuteIndex(index);
                using (var session = store.OpenAsyncSession())
                {
                    session.StoreAsync(new ReduceMe
                    {
                        Tag = "Foo",
                        Amount = 2
                    }).Wait();
                    session.StoreAsync(new ReduceMe
                    {
                        Tag = "Foo",
                        Amount = 3
                    }).Wait();
                    session.SaveChangesAsync().Wait();
                    WaitForIndexing(store);
                    var query = session.Query<ReduceMe>(index.IndexName);
                    using (var stream = session.Advanced.StreamAsync(query).Result)
                    {
                        var streamNotEmpty = false;
                        while (stream.MoveNextAsync().Result)
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
