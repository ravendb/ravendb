using System.Linq;
using System.Threading.Tasks;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Raven.Client;
using Raven.Client.Linq;

namespace Raven.Tests.Core
{
    public class MultiGetQueries : RavenTestBase
    {
        [Fact]
        public async Task UnlessAccessedLazyQueriesAreNoOp()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var result1 = session.Query<User>().Where(x => x.Name == "oren").Lazily();
                    var result2 = session.Query<User>().Where(x => x.Name == "ayende").Lazily();
                    Assert.Equal(0, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task WithPaging()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "oren" });
                    session.Store(new User());
                    session.Store(new User { Name = "ayende" });
                    session.Store(new User());
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result1 = session.Query<User>().Where(x => x.Age == 0).Skip(1).Take(2).Lazily();
                    Assert.Equal(2, result1.Value.ToArray().Length);
                }
            }
        }
    }
}
