using System.Linq;
using System.Threading.Tasks;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Raven.Client;
using Raven.Client.Linq;

namespace Raven.Tests.Core
{
    public class MultiGetOperations : RavenTestBase
    {
        [Fact]
        public async Task UnlessAccessedLazyLoadsAreNoOp()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Advanced.Lazily.Load<User>("users/1");
                    session.Advanced.Lazily.Load<User>("users/2");
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
                    var u1 = session.Advanced.Lazily.Load<User>(new[] { "users/1" });
                    var u2 = session.Advanced.Lazily.Load<User>(new[] { "users/2" });

                    Assert.Null(u1.Value[0]);
                    Assert.Null(u2.Value[0]);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }
    }
}
