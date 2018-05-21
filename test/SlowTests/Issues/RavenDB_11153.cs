using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11153 : RavenTestBase
    {
        [Fact]
        public async Task Should_mark_idle_index_as_normal()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "arek"
                    });

                    session.SaveChanges();

                    var users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Name == "arek").ToList();

                    Assert.Equal(1, users.Count);
                }

                var db = await GetDatabase(store.Database);

                var autoIndex = db.IndexStore.GetIndexes().First();

                autoIndex.SetState(IndexState.Idle);

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Name == "arek").ToList();

                    Assert.Equal(1, users.Count);
                }

                Assert.Equal(IndexState.Normal, autoIndex.State);
            }
        }
    }
}
