using System.Threading.Tasks;
using FastTests;
using Raven.Client.Exceptions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_9576 : RavenTestBase
    {
        [Fact]
        public async Task IdentitiesShouldNotOverwriteExistingDocuments()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 5; i++)
                    {
                        await session.StoreAsync(new User(), "users/" + i);
                    }
                    await session.SaveChangesAsync();

                    await session.StoreAsync(new User
                    {
                        Name = "Fitzchak"
                    }, "users|");
                    var concurrencyException = await Assert.ThrowsAsync<ConcurrencyException>(async () => await session.SaveChangesAsync());
                    Assert.Contains("Document users/1 has change vector ", concurrencyException.Message);
                    Assert.Contains(", but Put was called with expecting new document. Optimistic concurrency violation, transaction will be aborted.", concurrencyException.Message);
                    
                    await session.StoreAsync(new User
                    {
                        Name = "Fitzchak"
                    }, "ExpectedChangeVector", "users|");
                    var exception = await Assert.ThrowsAsync<RavenException>(async () => await session.SaveChangesAsync());
                    Assert.Contains("System.InvalidOperationException: You cannot use change vector (ExpectedChangeVector) when using identity in the document ID (users/2).", exception.Message);
                }
            }
        }

        private class User
        {
            public string Name { get; set; }
        }
    }
}
