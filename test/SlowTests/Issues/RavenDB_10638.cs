using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10638 : RavenTestBase
    {
        public RavenDB_10638(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task AfterQueryExecutedShouldBeExecutedOnlyOnce()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var counter = 0;

                    var results = session
                        .Query<User>()
                        .Customize(x => x.AfterQueryExecuted(r => { Interlocked.Increment(ref counter); }))
                        .Where(x => x.Name == "Doe")
                        .ToList();

                    Assert.Equal(0, results.Count);
                    Assert.Equal(1, counter);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var counter = 0;

                    var results = await session
                        .Query<User>()
                        .Customize(x => x.AfterQueryExecuted(r => { Interlocked.Increment(ref counter); }))
                        .Where(x => x.Name == "Doe")
                        .ToListAsync();

                    Assert.Equal(0, results.Count);
                    Assert.Equal(1, counter);
                }

                using (var session = store.OpenSession())
                {
                    var counter = 0;

                    var query = session
                        .Advanced
                        .DocumentQuery<User>();

                    query.AfterQueryExecuted(r => { Interlocked.Increment(ref counter); });

                    var results = query.WhereEquals("Name", "Doe")
                        .ToList();

                    Assert.Equal(0, results.Count);
                    Assert.Equal(1, counter);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var counter = 0;

                    var query = session
                        .Advanced
                        .AsyncDocumentQuery<User>();

                    query.AfterQueryExecuted(r => { Interlocked.Increment(ref counter); });

                    var results = await query.WhereEquals("Name", "Doe")
                        .ToListAsync();

                    Assert.Equal(0, results.Count);
                    Assert.Equal(1, counter);
                }
            }
        }
    }
}
