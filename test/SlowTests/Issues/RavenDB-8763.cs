using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDb8763 : RavenTestBase
    {
        public RavenDb8763(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanAsyncStreamQueryWithMapReduceResult()
        {
            using (var store = GetDocumentStore())
            {
                var index = new ReduceMeByTag();
                await store.ExecuteIndexAsync(index);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new ReduceMe
                    {
                        Tag = "Foo",
                        Amount = 2
                    });
                    await session.StoreAsync(new ReduceMe
                    {
                        Tag = "Foo",
                        Amount = 3
                    });
                    await session.SaveChangesAsync();
                    Indexes.WaitForIndexing(store);
                    var query = session.Query<ReduceMe>(index.IndexName);
                    await using (var stream = await session.Advanced.StreamAsync(query))
                    {
                        var streamNotEmpty = false;
                        while (await stream.MoveNextAsync())
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
