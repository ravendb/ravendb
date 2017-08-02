using System.Threading.Tasks;
using Raven.Client.Documents;
using Xunit;
using System.Linq;

namespace FastTests.Server.Documents.Indexing
{
    public class FullTextSearchOnAutoIndex : RavenTestBase
    {
        public class User
        {
            public string Name;
        }

        [Fact]
        public async Task CanUseFullTextSearchInAutoIndex()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenAsyncSession())
                {
                    await s.StoreAsync(new User
                    {
                        Name = "Ayende Rahien"
                    });

                    await s.SaveChangesAsync();
                }

                using (var s = store.OpenAsyncSession())
                {
                    var count = await s.Query<User>()
                        .Search(u => u.Name, "Ayende")
                        .CountAsync();
                    Assert.Equal(1, count);
                }

                using (var s = store.OpenAsyncSession())
                {
                    var count = await s.Query<User>()
                        .Where(u => u.Name == "Ayende")
                        .CountAsync();
                    Assert.Equal(0, count);
                }
            }
        }
    }
}
