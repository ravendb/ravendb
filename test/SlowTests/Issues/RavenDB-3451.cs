using System.Threading.Tasks;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_3451 : RavenTestBase
    {
        [Fact]
        public async Task GetMetadataForAsyncForAsyncSession()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    var entity = new User { Name = "John", Email = "Johnson@gmail.com" };
                    await session.StoreAsync(entity);
                    await session.SaveChangesAsync();

                    var metaData = await session.Advanced.GetMetadataForAsync(entity);

                    Assert.NotNull(metaData);
                }
            }
        }

        private class User
        {
            public string Name { get; set; }
            public string Email { get; set; }
        }
    }
}

