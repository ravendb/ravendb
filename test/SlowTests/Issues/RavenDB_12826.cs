using System;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12826 : RavenTestBase
    {
        [Fact]
        public async Task ToArrayAsyncShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company
                    {
                        Name = "HR"
                    });

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var companies = await session
                        .Query<Company>()
                        .ToArrayAsync();

                    Assert.Equal(1, companies.Length);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var companies = await session
                        .Advanced
                        .AsyncDocumentQuery<Company>()
                        .ToArrayAsync();

                    Assert.Equal(1, companies.Length);
                }

                using (var session = store.OpenSession())
                {
                    await Assert.ThrowsAsync<NotSupportedException>(() => session.Query<Company>().ToArrayAsync());
                }
            }
        }
    }
}
