using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Exceptions.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16321 : RavenTestBase
    {
        public RavenDB_16321(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Sharding)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task StreamingOnIndexThatDoesNotExistShouldThrow(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var stream = new MemoryStream())
                using (var session = store.OpenSession())
                {
                    Assert.Throws<IndexDoesNotExistException>(() =>
                    {
                        session.Query<Employee>("Does_Not_Exist")
                           .Where(x => x.FirstName == "Robert")
                           .ToStream(stream);
                    });
                }

                using (var stream = new MemoryStream())
                using (var session = store.OpenAsyncSession())
                {
                    await Assert.ThrowsAsync<IndexDoesNotExistException>(() =>
                    {
                        return session.Query<Employee>("Does_Not_Exist")
                           .Where(x => x.FirstName == "Robert")
                           .ToStreamAsync(stream);
                    });
                }
            }
        }
    }
}
